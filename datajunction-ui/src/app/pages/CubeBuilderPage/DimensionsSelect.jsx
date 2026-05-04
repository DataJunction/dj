/**
 * A select component for picking dimensions.
 * Dimensions are grouped by hop distance (how many joins away from the metrics).
 */
import Select from 'react-select';
import React, { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import { labelize } from '../../../utils/form';

/**
 * Calculate hop distance from path length.
 * path.length represents how many joins away the dimension is.
 */
const getHopDistance = path => {
  if (!path || path.length === 0) return 0;
  return path.length;
};

/**
 * Render role information as a label suffix when a dimension is reached
 * via a named role (e.g. "[birth_country]"). Stored role values can be raw
 * (e.g. "birth_country") or already bracketed; normalize either form to
 * " [role]". Returns "" when there is no role.
 */
const formatRoleSuffix = role => {
  if (!role) return '';
  const stripped = role.replace(/^\[|\]$/g, '');
  return stripped ? ` [${stripped}]` : '';
};

/**
 * Parse a role suffix off the end of a dimension's full name.
 * "default.user_dim.country_code[birth_country]" → ["default.user_dim.country_code", "birth_country"]
 * "default.user_dim.country_code"                → ["default.user_dim.country_code", ""]
 */
const splitRole = fullName => {
  if (!fullName) return [fullName, ''];
  const match = fullName.match(/^(.+?)\[([^\]]+)\]$/);
  return match ? [match[1], match[2]] : [fullName, ''];
};

/**
 * Get human-readable label for hop distance.
 */
const getHopLabel = hopDistance => {
  if (hopDistance === 0) return 'Direct Dimensions';
  if (hopDistance === 1) return '1 Hop Away';
  return `${hopDistance} Hops Away`;
};

export const DimensionsSelect = React.memo(function DimensionsSelect({
  cube,
  metrics,
  onChange,
}) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;

  // Dimensions grouped by hop distance, then by node+path
  const [dimensionsByHop, setDimensionsByHop] = useState({});

  // The selected dimensions, grouped by dimension node and path
  const [selectedDimensionsByGroup, setSelectedDimensionsByGroup] = useState(
    {},
  );

  // The existing cube node's dimensions, if editing a cube
  const [defaultDimensions, setDefaultDimensions] = useState([]);

  useEffect(() => {
    const fetchData = async () => {
      let cubeDimensions = undefined;
      if (cube) {
        cubeDimensions = cube?.current.cubeDimensions.map(cubeDim => {
          return {
            value: cubeDim.name,
            label:
              labelize(cubeDim.attribute) +
              formatRoleSuffix(cubeDim.role) +
              (cubeDim.properties?.includes('primary_key') ? ' (PK)' : ''),
          };
        });
        setDefaultDimensions(cubeDimensions);
        onChange(cubeDimensions.map(m => m.value));
      }

      if (metrics && metrics.length > 0) {
        // Populate the common dimensions list based on the selected metrics
        const commonDimensions = await djClient.commonDimensions(metrics);

        // First group by node_name + path (original grouping)
        const groupedByNodePath = commonDimensions.reduce(
          (group, dimension) => {
            const key = dimension.node_name + JSON.stringify(dimension.path);
            group[key] = group[key] ?? [];
            group[key].push(dimension);
            return group;
          },
          {},
        );

        // Then organize by hop distance
        const byHop = {};
        Object.values(groupedByNodePath).forEach(dimensionsInGroup => {
          const hopDistance = getHopDistance(dimensionsInGroup[0].path);
          byHop[hopDistance] = byHop[hopDistance] ?? [];
          byHop[hopDistance].push(dimensionsInGroup);
        });

        setDimensionsByHop(byHop);

        // Set the selected cube dimensions if an existing cube is being edited
        if (cube) {
          const currentSelectedDimensionsByGroup = {};
          Object.values(groupedByNodePath).forEach(dimensionsInGroup => {
            const groupKey =
              dimensionsInGroup[0].node_name +
              JSON.stringify(dimensionsInGroup[0].path);
            currentSelectedDimensionsByGroup[groupKey] = getValue(
              cubeDimensions.filter(
                dim =>
                  dimensionsInGroup.filter(x => dim.value === x.name).length >
                  0,
              ),
            );
          });
          setSelectedDimensionsByGroup(currentSelectedDimensionsByGroup);
          onChange(Object.values(currentSelectedDimensionsByGroup).flat(2));
        }
      } else {
        setDimensionsByHop({});
      }
    };
    fetchData().catch(console.error);
  }, [djClient, onChange, metrics, cube]);

  // Retrieves the selected values as a list (since it is a multi-select)
  const getValue = options => {
    if (options) {
      return options.map(option => option.value);
    } else {
      return [];
    }
  };

  // Sort hop distances numerically
  const sortedHops = Object.keys(dimensionsByHop)
    .map(Number)
    .sort((a, b) => a - b);

  // Custom styles to color-code dimension tags (matching Query Planner exactly)
  const dimensionStyles = {
    multiValue: base => ({
      ...base,
      backgroundColor: '#ffefd0',
      border: '1px solid rgba(169, 102, 33, 0.3)',
      borderRadius: '3px',
      margin: '2px',
    }),
    multiValueLabel: base => ({
      ...base,
      color: '#a96621',
      fontSize: '10px',
      fontWeight: 500,
      padding: '2px 4px 2px 6px',
    }),
    multiValueRemove: base => ({
      ...base,
      color: '#a96621',
      padding: '0 4px',
      ':hover': {
        backgroundColor: '#ffe4b3',
        color: '#a96621',
      },
    }),
  };

  if (sortedHops.length === 0) {
    return null;
  }

  return (
    <div>
      {sortedHops.map(hopDistance => {
        const groupsAtHop = dimensionsByHop[hopDistance];
        const dimensionCount = groupsAtHop.reduce(
          (sum, g) => sum + g.length,
          0,
        );

        return (
          <div key={hopDistance} style={{ marginBottom: '20px' }}>
            {/* Hop distance header */}
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                padding: '6px 0',
                borderBottom: '1px solid #dee2e6',
                marginBottom: '8px',
                marginTop: hopDistance > 0 ? '24px' : '0',
              }}
            >
              <span
                style={{
                  fontWeight: 600,
                  color: '#212529',
                  fontSize: '14px',
                }}
              >
                {getHopLabel(hopDistance)}
              </span>
              <span
                style={{
                  marginLeft: '10px',
                  color: '#6c757d',
                  fontSize: '13px',
                }}
              >
                ({dimensionCount} dimension{dimensionCount !== 1 ? 's' : ''})
              </span>
            </div>

            {/* Dimension groups within this hop - indented */}
            <div style={{ paddingLeft: '6px' }}>
              {groupsAtHop.map(dimensionsInGroup => {
                const groupKey =
                  dimensionsInGroup[0].node_name +
                  JSON.stringify(dimensionsInGroup[0].path);

                // Convert path node names to display names
                const pathDisplayNames = dimensionsInGroup[0].path.map(
                  nodeName => {
                    const lastSegment = nodeName.split('.').pop();
                    return labelize(lastSegment);
                  },
                );

                const groupHeader = (
                  <h5
                    style={{
                      fontWeight: 'normal',
                      marginBottom: '5px',
                      marginTop: '10px',
                      fontSize: '13px',
                    }}
                    title={dimensionsInGroup[0].path.join(' → ')}
                  >
                    <a href={`/nodes/${dimensionsInGroup[0].node_name}`}>
                      <b>{dimensionsInGroup[0].node_display_name}</b>
                    </a>
                    {pathDisplayNames.length > 0 && (
                      <>
                        {' '}
                        <span style={{ color: '#6c757d' }}>via</span>{' '}
                        {pathDisplayNames.map((displayName, idx) => (
                          <span key={idx}>
                            {idx > 0 && ' → '}
                            <a
                              href={`/nodes/${dimensionsInGroup[0].path[idx]}`}
                            >
                              {displayName}
                            </a>
                          </span>
                        ))}
                      </>
                    )}
                  </h5>
                );

                const dimensionGroupOptions = dimensionsInGroup.map(dim => {
                  const [bareName, role] = splitRole(dim.name);
                  return {
                    value: dim.name,
                    label:
                      labelize(bareName.split('.').slice(-1)[0]) +
                      formatRoleSuffix(role) +
                      (dim.properties?.includes('primary_key') ? ' (PK)' : ''),
                  };
                });

                const cubeDimensions = defaultDimensions.filter(
                  dim =>
                    dimensionGroupOptions.filter(x => dim.value === x.value)
                      .length > 0,
                );

                return (
                  <div key={groupKey}>
                    {groupHeader}
                    <span
                      data-testid={
                        'dimensions-' + dimensionsInGroup[0].node_name
                      }
                    >
                      <Select
                        className=""
                        name={'dimensions-' + groupKey}
                        defaultValue={cubeDimensions}
                        options={dimensionGroupOptions}
                        styles={dimensionStyles}
                        isMulti
                        isClearable
                        closeMenuOnSelect={false}
                        onChange={selected => {
                          const newSelected = {
                            ...selectedDimensionsByGroup,
                            [groupKey]: getValue(selected),
                          };
                          setSelectedDimensionsByGroup(newSelected);
                          onChange(Object.values(newSelected).flat(2));
                        }}
                      />
                    </span>
                  </div>
                );
              })}
            </div>
          </div>
        );
      })}
    </div>
  );
});
