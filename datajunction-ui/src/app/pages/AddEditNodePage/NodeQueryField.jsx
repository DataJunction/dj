/**
 * SQL query input field, which consists of a CodeMirror SQL editor with autocompletion
 * (for node names and columns) and syntax highlighting.
 */
import React from 'react';
import { ErrorMessage, Field, useFormikContext } from 'formik';
import CodeMirror from '@uiw/react-codemirror';
import { langs } from '@uiw/codemirror-extensions-langs';
import { extractCatalogTables } from './catalogTables';
import {
  djNodeBadges,
  djNodeHoverTooltip,
  refreshBadges,
} from './djNodeBadges';
import { djEditorExtensions } from './djEditorTheme';

export const NodeQueryField = ({ djClient, value }) => {
  const [schema, setSchema] = React.useState([]);
  const formik = useFormikContext();
  const sqlExt = langs.sql({ schema: schema });
  const autoRegisterTimer = React.useRef(null);
  const validateTimer = React.useRef(null);
  const registeredTables = React.useRef(new Set());
  // useRef so the onChange closure always sees the latest catalog list
  const knownCatalogsRef = React.useRef([]);

  // Per-`catalog.schema.table` registration status, surfaced as badges in
  // the editor. Mirrored into a ref so the CodeMirror extension (built once,
  // memoized below) can read the latest value without rebuilding.
  const [tableStatus, setTableStatus] = React.useState({});
  const tableStatusRef = React.useRef(tableStatus);
  React.useEffect(() => {
    tableStatusRef.current = tableStatus;
  }, [tableStatus]);
  const editorViewRef = React.useRef(null);
  const setStatus = React.useCallback((key, status) => {
    // Update the ref synchronously so the ViewPlugin's getStatus() closure
    // sees the new value on the redraw we're about to dispatch.
    // setTableStatus only schedules the React rerender (next tick) which is
    // too late for the dispatch below.
    tableStatusRef.current = { ...tableStatusRef.current, [key]: status };
    setTableStatus(tableStatusRef.current);
    if (editorViewRef.current) {
      editorViewRef.current.dispatch({ effects: refreshBadges.of(null) });
    }
  }, []);

  React.useEffect(() => {
    if (typeof djClient.catalogs !== 'function') return;
    Promise.resolve(djClient.catalogs())
      .then(catalogs => {
        knownCatalogsRef.current = (catalogs || []).map(c =>
          c.name.toLowerCase(),
        );
      })
      .catch(() => {
        // Auto-register is best-effort; failing to load catalogs just disables it.
      });
  }, [djClient]);

  const initialAutocomplete = async context => {
    // Based on the parsed prefix, we load node names with that prefix
    // into the autocomplete schema. At this stage we don't load the columns
    // to save on unnecessary calls
    const word = context.matchBefore(/[\.\w]*/);
    const matches = await djClient.nodes(word.text);
    matches.forEach(nodeName => {
      if (schema[nodeName] === undefined) {
        schema[nodeName] = [];
        setSchema(schema);
      }
    });
  };

  const updateFormik = val => {
    formik.setFieldValue('query', val);
  };

  const updateAutocomplete = async (value, _) => {
    // If a particular node has been chosen, load the columns of that node into
    // the autocomplete schema for column-level autocompletion
    for (var nodeName in schema) {
      if (
        value.includes(nodeName) &&
        (!schema.hasOwnProperty(nodeName) ||
          (schema.hasOwnProperty(nodeName) && schema[nodeName].length === 0))
      ) {
        const nodeDetails = await djClient.node(nodeName);
        schema[nodeName] = nodeDetails.columns.map(col => col.name);
        setSchema(schema);
      }
    }

    // Auto-register any catalog-qualified tables typed in the SQL
    if (knownCatalogsRef.current.length > 0) {
      clearTimeout(autoRegisterTimer.current);
      autoRegisterTimer.current = setTimeout(
        () => autoRegisterCatalogTables(value),
        600,
      );
    }

    // Validate the in-progress query against DJ to discover which 2-part
    // refs (`namespace.name`) resolve to real nodes vs. are missing.
    clearTimeout(validateTimer.current);
    validateTimer.current = setTimeout(() => validateRefs(value), 700);
  };

  const validateRefs = async sql => {
    if (typeof djClient.validateNode !== 'function') return;
    const fv = formik.values || {};
    if (!fv.type) return; // need a node type to validate
    let response;
    try {
      response = await djClient.validateNode(
        fv.type,
        fv.name || '__draft__',
        fv.display_name || 'Draft',
        fv.description || '',
        sql,
      );
    } catch {
      return; // best-effort; ignore network failures
    }
    const json = response?.json || {};
    const deps = json.dependencies || [];
    const missing = json.missing_parents || [];
    // Update the ref synchronously in one shot before dispatching, so the
    // ViewPlugin only redraws once even though we set many keys.
    const next = { ...tableStatusRef.current };
    for (const dep of deps) {
      // `dependencies` is a list of full node objects ({name, type, status, ...}).
      // Use `type` to color the chip per node kind (source/dimension/…) and
      // `status` to flag nodes that resolve but are themselves unhealthy
      // (e.g. broken upstream, drifted columns). We also keep the whole
      // object around so the hover popover can show description / version
      // / mode without re-fetching.
      const name = typeof dep === 'string' ? dep : dep?.name;
      if (!name) continue;
      const nodeType = typeof dep === 'object' ? dep.type : undefined;
      const nodeStatus = typeof dep === 'object' ? dep.status : undefined;
      const kind = nodeStatus && nodeStatus !== 'valid' ? 'warning' : 'valid';
      next[name] = {
        kind,
        refType: nodeType || 'node',
        node: typeof dep === 'object' ? dep : undefined,
        message:
          kind === 'warning' ? `Node \`${name}\` is ${nodeStatus}` : undefined,
      };
    }
    for (const m of missing) {
      // `missing_parents` is a list of bare name strings.
      const name = typeof m === 'string' ? m : m?.name;
      if (!name) continue;
      next[name] = {
        kind: 'invalid',
        refType: 'node',
        message: `Node \`${name}\` not found`,
      };
    }
    tableStatusRef.current = next;
    setTableStatus(next);
    if (editorViewRef.current) {
      editorViewRef.current.dispatch({ effects: refreshBadges.of(null) });
    }
  };

  const autoRegisterCatalogTables = async sql => {
    const tables = extractCatalogTables(sql, knownCatalogsRef.current);
    for (const [catalog, tableSchema, table] of tables) {
      const key = `${catalog}.${tableSchema}.${table}`;
      if (registeredTables.current.has(key)) continue;
      // If autocomplete already saw this node, DJ knows about it — skip silently
      if (schema[key] !== undefined) {
        registeredTables.current.add(key);
        setStatus(key, { kind: 'valid' });
        continue;
      }
      registeredTables.current.add(key);
      setStatus(key, { kind: 'registering' });

      let response;
      try {
        response = await djClient.registerTable(
          catalog,
          tableSchema,
          table,
          '',
        );
      } catch (err) {
        registeredTables.current.delete(key);
        setStatus(key, {
          kind: 'invalid',
          message: err?.message || 'Failed to register table',
        });
        continue;
      }
      const { status, json } = response;
      if (status === 200 || status === 201) {
        const nodeName = json?.name || key;
        const columns = (json?.columns || []).map(col => col.name);
        schema[nodeName] = columns;
        if (table && table !== nodeName) {
          schema[table] = columns;
        }
        setSchema(schema);
        setStatus(key, { kind: 'valid' });
      } else if (status === 409) {
        // 409 = already exists; treat as valid.
        setStatus(key, { kind: 'valid' });
      } else {
        registeredTables.current.delete(key);
        setStatus(key, {
          kind: 'invalid',
          message: json?.message || `Registration failed (HTTP ${status})`,
        });
      }
    }
  };

  const badgeExt = React.useMemo(
    () =>
      djNodeBadges({
        getStatus: key => tableStatusRef.current[key],
        getKnownCatalogs: () => knownCatalogsRef.current,
      }),
    [],
  );

  const hoverExt = React.useMemo(
    () =>
      djNodeHoverTooltip({
        getStatus: key => tableStatusRef.current[key],
        getKnownCatalogs: () => knownCatalogsRef.current,
        fetchNodeDetails: name => djClient.node(name).catch(() => null),
      }),
    [djClient],
  );

  return (
    <div className="QueryInput NodeCreationInput">
      <ErrorMessage name="query" component="span" />
      <label htmlFor="Query">Query *</label>
      <Field
        type="textarea"
        style={{ display: 'none' }}
        as="textarea"
        name="query"
        id="Query"
      />
      <div role="button" tabIndex={0} className="relative flex bg-[#282a36]">
        <CodeMirror
          id={'query'}
          name={'query'}
          extensions={[
            sqlExt,
            sqlExt.language.data.of({
              autocomplete: initialAutocomplete,
            }),
            ...djEditorExtensions,
            badgeExt,
            hoverExt,
          ]}
          onCreateEditor={view => {
            editorViewRef.current = view;
          }}
          value={value}
          placeholder={
            'SELECT\n\tprimary_key,\n\tmeasure1,\n\tmeasure2,\n\tforeign_key_for_dimension1,\n\tforeign_key_for_dimension2\nFROM source.source_node\nWHERE ...'
          }
          options={{
            theme: 'default',
            lineNumbers: true,
          }}
          width="100%"
          height="400px"
          style={{
            margin: '0 0 23px 0',
            flex: 1,
            fontSize: '110%',
            textAlign: 'left',
          }}
          onChange={(value, viewUpdate) => {
            updateFormik(value);
            updateAutocomplete(value, viewUpdate);
          }}
        />
      </div>
    </div>
  );
};
