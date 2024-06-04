# Generated from SqlBaseParser.g4 by ANTLR 4.13.1
from antlr4 import *
if "." in __name__:
    from .SqlBaseParser import SqlBaseParser
else:
    from SqlBaseParser import SqlBaseParser

# This class defines a complete generic visitor for a parse tree produced by SqlBaseParser.

class SqlBaseParserVisitor(ParseTreeVisitor):

    # Visit a parse tree produced by SqlBaseParser#singleStatement.
    def visitSingleStatement(self, ctx:SqlBaseParser.SingleStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#singleExpression.
    def visitSingleExpression(self, ctx:SqlBaseParser.SingleExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#singleTableIdentifier.
    def visitSingleTableIdentifier(self, ctx:SqlBaseParser.SingleTableIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#singleMultipartIdentifier.
    def visitSingleMultipartIdentifier(self, ctx:SqlBaseParser.SingleMultipartIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#singleFunctionIdentifier.
    def visitSingleFunctionIdentifier(self, ctx:SqlBaseParser.SingleFunctionIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#singleDataType.
    def visitSingleDataType(self, ctx:SqlBaseParser.SingleDataTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#singleTableSchema.
    def visitSingleTableSchema(self, ctx:SqlBaseParser.SingleTableSchemaContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#statementDefault.
    def visitStatementDefault(self, ctx:SqlBaseParser.StatementDefaultContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#dmlStatement.
    def visitDmlStatement(self, ctx:SqlBaseParser.DmlStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#use.
    def visitUse(self, ctx:SqlBaseParser.UseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#useNamespace.
    def visitUseNamespace(self, ctx:SqlBaseParser.UseNamespaceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#setCatalog.
    def visitSetCatalog(self, ctx:SqlBaseParser.SetCatalogContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#createNamespace.
    def visitCreateNamespace(self, ctx:SqlBaseParser.CreateNamespaceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#setNamespaceProperties.
    def visitSetNamespaceProperties(self, ctx:SqlBaseParser.SetNamespacePropertiesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#setNamespaceLocation.
    def visitSetNamespaceLocation(self, ctx:SqlBaseParser.SetNamespaceLocationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#dropNamespace.
    def visitDropNamespace(self, ctx:SqlBaseParser.DropNamespaceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#showNamespaces.
    def visitShowNamespaces(self, ctx:SqlBaseParser.ShowNamespacesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#createTable.
    def visitCreateTable(self, ctx:SqlBaseParser.CreateTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#createTableLike.
    def visitCreateTableLike(self, ctx:SqlBaseParser.CreateTableLikeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#replaceTable.
    def visitReplaceTable(self, ctx:SqlBaseParser.ReplaceTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#analyze.
    def visitAnalyze(self, ctx:SqlBaseParser.AnalyzeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#analyzeTables.
    def visitAnalyzeTables(self, ctx:SqlBaseParser.AnalyzeTablesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#addTableColumns.
    def visitAddTableColumns(self, ctx:SqlBaseParser.AddTableColumnsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#renameTableColumn.
    def visitRenameTableColumn(self, ctx:SqlBaseParser.RenameTableColumnContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#dropTableColumns.
    def visitDropTableColumns(self, ctx:SqlBaseParser.DropTableColumnsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#renameTable.
    def visitRenameTable(self, ctx:SqlBaseParser.RenameTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#setTableProperties.
    def visitSetTableProperties(self, ctx:SqlBaseParser.SetTablePropertiesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#unsetTableProperties.
    def visitUnsetTableProperties(self, ctx:SqlBaseParser.UnsetTablePropertiesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#alterTableAlterColumn.
    def visitAlterTableAlterColumn(self, ctx:SqlBaseParser.AlterTableAlterColumnContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#hiveChangeColumn.
    def visitHiveChangeColumn(self, ctx:SqlBaseParser.HiveChangeColumnContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#hiveReplaceColumns.
    def visitHiveReplaceColumns(self, ctx:SqlBaseParser.HiveReplaceColumnsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#setTableSerDe.
    def visitSetTableSerDe(self, ctx:SqlBaseParser.SetTableSerDeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#addTablePartition.
    def visitAddTablePartition(self, ctx:SqlBaseParser.AddTablePartitionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#renameTablePartition.
    def visitRenameTablePartition(self, ctx:SqlBaseParser.RenameTablePartitionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#dropTablePartitions.
    def visitDropTablePartitions(self, ctx:SqlBaseParser.DropTablePartitionsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#setTableLocation.
    def visitSetTableLocation(self, ctx:SqlBaseParser.SetTableLocationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#recoverPartitions.
    def visitRecoverPartitions(self, ctx:SqlBaseParser.RecoverPartitionsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#dropTable.
    def visitDropTable(self, ctx:SqlBaseParser.DropTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#dropView.
    def visitDropView(self, ctx:SqlBaseParser.DropViewContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#createView.
    def visitCreateView(self, ctx:SqlBaseParser.CreateViewContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#createTempViewUsing.
    def visitCreateTempViewUsing(self, ctx:SqlBaseParser.CreateTempViewUsingContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#alterViewQuery.
    def visitAlterViewQuery(self, ctx:SqlBaseParser.AlterViewQueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#createFunction.
    def visitCreateFunction(self, ctx:SqlBaseParser.CreateFunctionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#dropFunction.
    def visitDropFunction(self, ctx:SqlBaseParser.DropFunctionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#explain.
    def visitExplain(self, ctx:SqlBaseParser.ExplainContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#showTables.
    def visitShowTables(self, ctx:SqlBaseParser.ShowTablesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#showTableExtended.
    def visitShowTableExtended(self, ctx:SqlBaseParser.ShowTableExtendedContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#showTblProperties.
    def visitShowTblProperties(self, ctx:SqlBaseParser.ShowTblPropertiesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#showColumns.
    def visitShowColumns(self, ctx:SqlBaseParser.ShowColumnsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#showViews.
    def visitShowViews(self, ctx:SqlBaseParser.ShowViewsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#showPartitions.
    def visitShowPartitions(self, ctx:SqlBaseParser.ShowPartitionsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#showFunctions.
    def visitShowFunctions(self, ctx:SqlBaseParser.ShowFunctionsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#showCreateTable.
    def visitShowCreateTable(self, ctx:SqlBaseParser.ShowCreateTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#showCurrentNamespace.
    def visitShowCurrentNamespace(self, ctx:SqlBaseParser.ShowCurrentNamespaceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#showCatalogs.
    def visitShowCatalogs(self, ctx:SqlBaseParser.ShowCatalogsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#describeFunction.
    def visitDescribeFunction(self, ctx:SqlBaseParser.DescribeFunctionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#describeNamespace.
    def visitDescribeNamespace(self, ctx:SqlBaseParser.DescribeNamespaceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#describeRelation.
    def visitDescribeRelation(self, ctx:SqlBaseParser.DescribeRelationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#describeQuery.
    def visitDescribeQuery(self, ctx:SqlBaseParser.DescribeQueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#commentNamespace.
    def visitCommentNamespace(self, ctx:SqlBaseParser.CommentNamespaceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#commentTable.
    def visitCommentTable(self, ctx:SqlBaseParser.CommentTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#refreshTable.
    def visitRefreshTable(self, ctx:SqlBaseParser.RefreshTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#refreshFunction.
    def visitRefreshFunction(self, ctx:SqlBaseParser.RefreshFunctionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#refreshResource.
    def visitRefreshResource(self, ctx:SqlBaseParser.RefreshResourceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#cacheTable.
    def visitCacheTable(self, ctx:SqlBaseParser.CacheTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#uncacheTable.
    def visitUncacheTable(self, ctx:SqlBaseParser.UncacheTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#clearCache.
    def visitClearCache(self, ctx:SqlBaseParser.ClearCacheContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#loadData.
    def visitLoadData(self, ctx:SqlBaseParser.LoadDataContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#truncateTable.
    def visitTruncateTable(self, ctx:SqlBaseParser.TruncateTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#repairTable.
    def visitRepairTable(self, ctx:SqlBaseParser.RepairTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#manageResource.
    def visitManageResource(self, ctx:SqlBaseParser.ManageResourceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#failNativeCommand.
    def visitFailNativeCommand(self, ctx:SqlBaseParser.FailNativeCommandContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#setTimeZone.
    def visitSetTimeZone(self, ctx:SqlBaseParser.SetTimeZoneContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#setQuotedConfiguration.
    def visitSetQuotedConfiguration(self, ctx:SqlBaseParser.SetQuotedConfigurationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#setConfiguration.
    def visitSetConfiguration(self, ctx:SqlBaseParser.SetConfigurationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#resetQuotedConfiguration.
    def visitResetQuotedConfiguration(self, ctx:SqlBaseParser.ResetQuotedConfigurationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#resetConfiguration.
    def visitResetConfiguration(self, ctx:SqlBaseParser.ResetConfigurationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#createIndex.
    def visitCreateIndex(self, ctx:SqlBaseParser.CreateIndexContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#dropIndex.
    def visitDropIndex(self, ctx:SqlBaseParser.DropIndexContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#timezone.
    def visitTimezone(self, ctx:SqlBaseParser.TimezoneContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#configKey.
    def visitConfigKey(self, ctx:SqlBaseParser.ConfigKeyContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#configValue.
    def visitConfigValue(self, ctx:SqlBaseParser.ConfigValueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#unsupportedHiveNativeCommands.
    def visitUnsupportedHiveNativeCommands(self, ctx:SqlBaseParser.UnsupportedHiveNativeCommandsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#createTableHeader.
    def visitCreateTableHeader(self, ctx:SqlBaseParser.CreateTableHeaderContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#replaceTableHeader.
    def visitReplaceTableHeader(self, ctx:SqlBaseParser.ReplaceTableHeaderContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#bucketSpec.
    def visitBucketSpec(self, ctx:SqlBaseParser.BucketSpecContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#skewSpec.
    def visitSkewSpec(self, ctx:SqlBaseParser.SkewSpecContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#locationSpec.
    def visitLocationSpec(self, ctx:SqlBaseParser.LocationSpecContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#commentSpec.
    def visitCommentSpec(self, ctx:SqlBaseParser.CommentSpecContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#insertOverwriteTable.
    def visitInsertOverwriteTable(self, ctx:SqlBaseParser.InsertOverwriteTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#insertIntoTable.
    def visitInsertIntoTable(self, ctx:SqlBaseParser.InsertIntoTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#insertIntoReplaceWhere.
    def visitInsertIntoReplaceWhere(self, ctx:SqlBaseParser.InsertIntoReplaceWhereContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#insertOverwriteHiveDir.
    def visitInsertOverwriteHiveDir(self, ctx:SqlBaseParser.InsertOverwriteHiveDirContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#insertOverwriteDir.
    def visitInsertOverwriteDir(self, ctx:SqlBaseParser.InsertOverwriteDirContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#partitionSpecLocation.
    def visitPartitionSpecLocation(self, ctx:SqlBaseParser.PartitionSpecLocationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#partitionSpec.
    def visitPartitionSpec(self, ctx:SqlBaseParser.PartitionSpecContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#partitionVal.
    def visitPartitionVal(self, ctx:SqlBaseParser.PartitionValContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#namespace.
    def visitNamespace(self, ctx:SqlBaseParser.NamespaceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#namespaces.
    def visitNamespaces(self, ctx:SqlBaseParser.NamespacesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#describeFuncName.
    def visitDescribeFuncName(self, ctx:SqlBaseParser.DescribeFuncNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#describeColName.
    def visitDescribeColName(self, ctx:SqlBaseParser.DescribeColNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#ctes.
    def visitCtes(self, ctx:SqlBaseParser.CtesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#query.
    def visitQuery(self, ctx:SqlBaseParser.QueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#namedQuery.
    def visitNamedQuery(self, ctx:SqlBaseParser.NamedQueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#queryTermDefault.
    def visitQueryTermDefault(self, ctx:SqlBaseParser.QueryTermDefaultContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#setOperation.
    def visitSetOperation(self, ctx:SqlBaseParser.SetOperationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#transformQuerySpecification.
    def visitTransformQuerySpecification(self, ctx:SqlBaseParser.TransformQuerySpecificationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#regularQuerySpecification.
    def visitRegularQuerySpecification(self, ctx:SqlBaseParser.RegularQuerySpecificationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#queryPrimaryDefault.
    def visitQueryPrimaryDefault(self, ctx:SqlBaseParser.QueryPrimaryDefaultContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#fromStmt.
    def visitFromStmt(self, ctx:SqlBaseParser.FromStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#table.
    def visitTable(self, ctx:SqlBaseParser.TableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#inlineTableDefault1.
    def visitInlineTableDefault1(self, ctx:SqlBaseParser.InlineTableDefault1Context):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#subquery.
    def visitSubquery(self, ctx:SqlBaseParser.SubqueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#tableProvider.
    def visitTableProvider(self, ctx:SqlBaseParser.TableProviderContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#createTableClauses.
    def visitCreateTableClauses(self, ctx:SqlBaseParser.CreateTableClausesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#propertyList.
    def visitPropertyList(self, ctx:SqlBaseParser.PropertyListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#property.
    def visitProperty(self, ctx:SqlBaseParser.PropertyContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#propertyKey.
    def visitPropertyKey(self, ctx:SqlBaseParser.PropertyKeyContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#propertyValue.
    def visitPropertyValue(self, ctx:SqlBaseParser.PropertyValueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#constantList.
    def visitConstantList(self, ctx:SqlBaseParser.ConstantListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#nestedConstantList.
    def visitNestedConstantList(self, ctx:SqlBaseParser.NestedConstantListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#createFileFormat.
    def visitCreateFileFormat(self, ctx:SqlBaseParser.CreateFileFormatContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#tableFileFormat.
    def visitTableFileFormat(self, ctx:SqlBaseParser.TableFileFormatContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#genericFileFormat.
    def visitGenericFileFormat(self, ctx:SqlBaseParser.GenericFileFormatContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#storageHandler.
    def visitStorageHandler(self, ctx:SqlBaseParser.StorageHandlerContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#resource.
    def visitResource(self, ctx:SqlBaseParser.ResourceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#singleInsertQuery.
    def visitSingleInsertQuery(self, ctx:SqlBaseParser.SingleInsertQueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#multiInsertQuery.
    def visitMultiInsertQuery(self, ctx:SqlBaseParser.MultiInsertQueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#deleteFromTable.
    def visitDeleteFromTable(self, ctx:SqlBaseParser.DeleteFromTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#updateTable.
    def visitUpdateTable(self, ctx:SqlBaseParser.UpdateTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#mergeIntoTable.
    def visitMergeIntoTable(self, ctx:SqlBaseParser.MergeIntoTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#queryOrganization.
    def visitQueryOrganization(self, ctx:SqlBaseParser.QueryOrganizationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#multiInsertQueryBody.
    def visitMultiInsertQueryBody(self, ctx:SqlBaseParser.MultiInsertQueryBodyContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#sortItem.
    def visitSortItem(self, ctx:SqlBaseParser.SortItemContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#fromStatement.
    def visitFromStatement(self, ctx:SqlBaseParser.FromStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#fromStatementBody.
    def visitFromStatementBody(self, ctx:SqlBaseParser.FromStatementBodyContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#transformClause.
    def visitTransformClause(self, ctx:SqlBaseParser.TransformClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#selectClause.
    def visitSelectClause(self, ctx:SqlBaseParser.SelectClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#setClause.
    def visitSetClause(self, ctx:SqlBaseParser.SetClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#matchedClause.
    def visitMatchedClause(self, ctx:SqlBaseParser.MatchedClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#notMatchedClause.
    def visitNotMatchedClause(self, ctx:SqlBaseParser.NotMatchedClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#notMatchedBySourceClause.
    def visitNotMatchedBySourceClause(self, ctx:SqlBaseParser.NotMatchedBySourceClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#matchedAction.
    def visitMatchedAction(self, ctx:SqlBaseParser.MatchedActionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#notMatchedAction.
    def visitNotMatchedAction(self, ctx:SqlBaseParser.NotMatchedActionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#notMatchedBySourceAction.
    def visitNotMatchedBySourceAction(self, ctx:SqlBaseParser.NotMatchedBySourceActionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#assignmentList.
    def visitAssignmentList(self, ctx:SqlBaseParser.AssignmentListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#assignment.
    def visitAssignment(self, ctx:SqlBaseParser.AssignmentContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#whereClause.
    def visitWhereClause(self, ctx:SqlBaseParser.WhereClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#havingClause.
    def visitHavingClause(self, ctx:SqlBaseParser.HavingClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#hint.
    def visitHint(self, ctx:SqlBaseParser.HintContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#hintStatement.
    def visitHintStatement(self, ctx:SqlBaseParser.HintStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#fromClause.
    def visitFromClause(self, ctx:SqlBaseParser.FromClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#temporalClause.
    def visitTemporalClause(self, ctx:SqlBaseParser.TemporalClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#aggregationClause.
    def visitAggregationClause(self, ctx:SqlBaseParser.AggregationClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#groupByClause.
    def visitGroupByClause(self, ctx:SqlBaseParser.GroupByClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#groupingAnalytics.
    def visitGroupingAnalytics(self, ctx:SqlBaseParser.GroupingAnalyticsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#groupingElement.
    def visitGroupingElement(self, ctx:SqlBaseParser.GroupingElementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#groupingSet.
    def visitGroupingSet(self, ctx:SqlBaseParser.GroupingSetContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#pivotClause.
    def visitPivotClause(self, ctx:SqlBaseParser.PivotClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#pivotColumn.
    def visitPivotColumn(self, ctx:SqlBaseParser.PivotColumnContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#pivotValue.
    def visitPivotValue(self, ctx:SqlBaseParser.PivotValueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#unpivotClause.
    def visitUnpivotClause(self, ctx:SqlBaseParser.UnpivotClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#unpivotNullClause.
    def visitUnpivotNullClause(self, ctx:SqlBaseParser.UnpivotNullClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#unpivotOperator.
    def visitUnpivotOperator(self, ctx:SqlBaseParser.UnpivotOperatorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#unpivotSingleValueColumnClause.
    def visitUnpivotSingleValueColumnClause(self, ctx:SqlBaseParser.UnpivotSingleValueColumnClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#unpivotMultiValueColumnClause.
    def visitUnpivotMultiValueColumnClause(self, ctx:SqlBaseParser.UnpivotMultiValueColumnClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#unpivotColumnSet.
    def visitUnpivotColumnSet(self, ctx:SqlBaseParser.UnpivotColumnSetContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#unpivotValueColumn.
    def visitUnpivotValueColumn(self, ctx:SqlBaseParser.UnpivotValueColumnContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#unpivotNameColumn.
    def visitUnpivotNameColumn(self, ctx:SqlBaseParser.UnpivotNameColumnContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#unpivotColumnAndAlias.
    def visitUnpivotColumnAndAlias(self, ctx:SqlBaseParser.UnpivotColumnAndAliasContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#unpivotColumn.
    def visitUnpivotColumn(self, ctx:SqlBaseParser.UnpivotColumnContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#unpivotAlias.
    def visitUnpivotAlias(self, ctx:SqlBaseParser.UnpivotAliasContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#lateralView.
    def visitLateralView(self, ctx:SqlBaseParser.LateralViewContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#setQuantifier.
    def visitSetQuantifier(self, ctx:SqlBaseParser.SetQuantifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#relation.
    def visitRelation(self, ctx:SqlBaseParser.RelationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#relationExtension.
    def visitRelationExtension(self, ctx:SqlBaseParser.RelationExtensionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#joinRelation.
    def visitJoinRelation(self, ctx:SqlBaseParser.JoinRelationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#joinType.
    def visitJoinType(self, ctx:SqlBaseParser.JoinTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#joinCriteria.
    def visitJoinCriteria(self, ctx:SqlBaseParser.JoinCriteriaContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#sample.
    def visitSample(self, ctx:SqlBaseParser.SampleContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#sampleByPercentile.
    def visitSampleByPercentile(self, ctx:SqlBaseParser.SampleByPercentileContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#sampleByRows.
    def visitSampleByRows(self, ctx:SqlBaseParser.SampleByRowsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#sampleByBucket.
    def visitSampleByBucket(self, ctx:SqlBaseParser.SampleByBucketContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#sampleByBytes.
    def visitSampleByBytes(self, ctx:SqlBaseParser.SampleByBytesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#identifierList.
    def visitIdentifierList(self, ctx:SqlBaseParser.IdentifierListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#identifierSeq.
    def visitIdentifierSeq(self, ctx:SqlBaseParser.IdentifierSeqContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#orderedIdentifierList.
    def visitOrderedIdentifierList(self, ctx:SqlBaseParser.OrderedIdentifierListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#orderedIdentifier.
    def visitOrderedIdentifier(self, ctx:SqlBaseParser.OrderedIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#identifierCommentList.
    def visitIdentifierCommentList(self, ctx:SqlBaseParser.IdentifierCommentListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#identifierComment.
    def visitIdentifierComment(self, ctx:SqlBaseParser.IdentifierCommentContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#tableName.
    def visitTableName(self, ctx:SqlBaseParser.TableNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#aliasedQuery.
    def visitAliasedQuery(self, ctx:SqlBaseParser.AliasedQueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#aliasedRelation.
    def visitAliasedRelation(self, ctx:SqlBaseParser.AliasedRelationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#inlineTableDefault2.
    def visitInlineTableDefault2(self, ctx:SqlBaseParser.InlineTableDefault2Context):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#tableValuedFunction.
    def visitTableValuedFunction(self, ctx:SqlBaseParser.TableValuedFunctionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#inlineTable.
    def visitInlineTable(self, ctx:SqlBaseParser.InlineTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#functionTable.
    def visitFunctionTable(self, ctx:SqlBaseParser.FunctionTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#tableAlias.
    def visitTableAlias(self, ctx:SqlBaseParser.TableAliasContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#rowFormatSerde.
    def visitRowFormatSerde(self, ctx:SqlBaseParser.RowFormatSerdeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#rowFormatDelimited.
    def visitRowFormatDelimited(self, ctx:SqlBaseParser.RowFormatDelimitedContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#multipartIdentifierList.
    def visitMultipartIdentifierList(self, ctx:SqlBaseParser.MultipartIdentifierListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#multipartIdentifier.
    def visitMultipartIdentifier(self, ctx:SqlBaseParser.MultipartIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#multipartIdentifierPropertyList.
    def visitMultipartIdentifierPropertyList(self, ctx:SqlBaseParser.MultipartIdentifierPropertyListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#multipartIdentifierProperty.
    def visitMultipartIdentifierProperty(self, ctx:SqlBaseParser.MultipartIdentifierPropertyContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#tableIdentifier.
    def visitTableIdentifier(self, ctx:SqlBaseParser.TableIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#functionIdentifier.
    def visitFunctionIdentifier(self, ctx:SqlBaseParser.FunctionIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#namedExpression.
    def visitNamedExpression(self, ctx:SqlBaseParser.NamedExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#namedExpressionSeq.
    def visitNamedExpressionSeq(self, ctx:SqlBaseParser.NamedExpressionSeqContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#partitionFieldList.
    def visitPartitionFieldList(self, ctx:SqlBaseParser.PartitionFieldListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#partitionTransform.
    def visitPartitionTransform(self, ctx:SqlBaseParser.PartitionTransformContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#partitionColumn.
    def visitPartitionColumn(self, ctx:SqlBaseParser.PartitionColumnContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#identityTransform.
    def visitIdentityTransform(self, ctx:SqlBaseParser.IdentityTransformContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#applyTransform.
    def visitApplyTransform(self, ctx:SqlBaseParser.ApplyTransformContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#transformArgument.
    def visitTransformArgument(self, ctx:SqlBaseParser.TransformArgumentContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#expression.
    def visitExpression(self, ctx:SqlBaseParser.ExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#expressionSeq.
    def visitExpressionSeq(self, ctx:SqlBaseParser.ExpressionSeqContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#logicalNot.
    def visitLogicalNot(self, ctx:SqlBaseParser.LogicalNotContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#predicated.
    def visitPredicated(self, ctx:SqlBaseParser.PredicatedContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#exists.
    def visitExists(self, ctx:SqlBaseParser.ExistsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#logicalBinary.
    def visitLogicalBinary(self, ctx:SqlBaseParser.LogicalBinaryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#predicate.
    def visitPredicate(self, ctx:SqlBaseParser.PredicateContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#valueExpressionDefault.
    def visitValueExpressionDefault(self, ctx:SqlBaseParser.ValueExpressionDefaultContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#comparison.
    def visitComparison(self, ctx:SqlBaseParser.ComparisonContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#arithmeticBinary.
    def visitArithmeticBinary(self, ctx:SqlBaseParser.ArithmeticBinaryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#arithmeticUnary.
    def visitArithmeticUnary(self, ctx:SqlBaseParser.ArithmeticUnaryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#datetimeUnit.
    def visitDatetimeUnit(self, ctx:SqlBaseParser.DatetimeUnitContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#struct.
    def visitStruct(self, ctx:SqlBaseParser.StructContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#dereference.
    def visitDereference(self, ctx:SqlBaseParser.DereferenceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#timestampadd.
    def visitTimestampadd(self, ctx:SqlBaseParser.TimestampaddContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#substring.
    def visitSubstring(self, ctx:SqlBaseParser.SubstringContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#cast.
    def visitCast(self, ctx:SqlBaseParser.CastContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#lambda.
    def visitLambda(self, ctx:SqlBaseParser.LambdaContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#parenthesizedExpression.
    def visitParenthesizedExpression(self, ctx:SqlBaseParser.ParenthesizedExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#any_value.
    def visitAny_value(self, ctx:SqlBaseParser.Any_valueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#trim.
    def visitTrim(self, ctx:SqlBaseParser.TrimContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#simpleCase.
    def visitSimpleCase(self, ctx:SqlBaseParser.SimpleCaseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#currentLike.
    def visitCurrentLike(self, ctx:SqlBaseParser.CurrentLikeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#columnReference.
    def visitColumnReference(self, ctx:SqlBaseParser.ColumnReferenceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#rowConstructor.
    def visitRowConstructor(self, ctx:SqlBaseParser.RowConstructorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#last.
    def visitLast(self, ctx:SqlBaseParser.LastContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#star.
    def visitStar(self, ctx:SqlBaseParser.StarContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#overlay.
    def visitOverlay(self, ctx:SqlBaseParser.OverlayContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#subscript.
    def visitSubscript(self, ctx:SqlBaseParser.SubscriptContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#timestampdiff.
    def visitTimestampdiff(self, ctx:SqlBaseParser.TimestampdiffContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#subqueryExpression.
    def visitSubqueryExpression(self, ctx:SqlBaseParser.SubqueryExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#constantDefault.
    def visitConstantDefault(self, ctx:SqlBaseParser.ConstantDefaultContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#extract.
    def visitExtract(self, ctx:SqlBaseParser.ExtractContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#percentile.
    def visitPercentile(self, ctx:SqlBaseParser.PercentileContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#functionCall.
    def visitFunctionCall(self, ctx:SqlBaseParser.FunctionCallContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#searchedCase.
    def visitSearchedCase(self, ctx:SqlBaseParser.SearchedCaseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#position.
    def visitPosition(self, ctx:SqlBaseParser.PositionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#first.
    def visitFirst(self, ctx:SqlBaseParser.FirstContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#nullLiteral.
    def visitNullLiteral(self, ctx:SqlBaseParser.NullLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#parameterLiteral.
    def visitParameterLiteral(self, ctx:SqlBaseParser.ParameterLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#intervalLiteral.
    def visitIntervalLiteral(self, ctx:SqlBaseParser.IntervalLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#typeConstructor.
    def visitTypeConstructor(self, ctx:SqlBaseParser.TypeConstructorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#numericLiteral.
    def visitNumericLiteral(self, ctx:SqlBaseParser.NumericLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#booleanLiteral.
    def visitBooleanLiteral(self, ctx:SqlBaseParser.BooleanLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#stringLiteral.
    def visitStringLiteral(self, ctx:SqlBaseParser.StringLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#comparisonOperator.
    def visitComparisonOperator(self, ctx:SqlBaseParser.ComparisonOperatorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#arithmeticOperator.
    def visitArithmeticOperator(self, ctx:SqlBaseParser.ArithmeticOperatorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#predicateOperator.
    def visitPredicateOperator(self, ctx:SqlBaseParser.PredicateOperatorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#booleanValue.
    def visitBooleanValue(self, ctx:SqlBaseParser.BooleanValueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#interval.
    def visitInterval(self, ctx:SqlBaseParser.IntervalContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#errorCapturingMultiUnitsInterval.
    def visitErrorCapturingMultiUnitsInterval(self, ctx:SqlBaseParser.ErrorCapturingMultiUnitsIntervalContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#multiUnitsInterval.
    def visitMultiUnitsInterval(self, ctx:SqlBaseParser.MultiUnitsIntervalContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#errorCapturingUnitToUnitInterval.
    def visitErrorCapturingUnitToUnitInterval(self, ctx:SqlBaseParser.ErrorCapturingUnitToUnitIntervalContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#unitToUnitInterval.
    def visitUnitToUnitInterval(self, ctx:SqlBaseParser.UnitToUnitIntervalContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#intervalValue.
    def visitIntervalValue(self, ctx:SqlBaseParser.IntervalValueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#unitInMultiUnits.
    def visitUnitInMultiUnits(self, ctx:SqlBaseParser.UnitInMultiUnitsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#unitInUnitToUnit.
    def visitUnitInUnitToUnit(self, ctx:SqlBaseParser.UnitInUnitToUnitContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#colPosition.
    def visitColPosition(self, ctx:SqlBaseParser.ColPositionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#complexDataType.
    def visitComplexDataType(self, ctx:SqlBaseParser.ComplexDataTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#yearMonthIntervalDataType.
    def visitYearMonthIntervalDataType(self, ctx:SqlBaseParser.YearMonthIntervalDataTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#dayTimeIntervalDataType.
    def visitDayTimeIntervalDataType(self, ctx:SqlBaseParser.DayTimeIntervalDataTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#primitiveDataType.
    def visitPrimitiveDataType(self, ctx:SqlBaseParser.PrimitiveDataTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#qualifiedColTypeWithPositionList.
    def visitQualifiedColTypeWithPositionList(self, ctx:SqlBaseParser.QualifiedColTypeWithPositionListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#qualifiedColTypeWithPosition.
    def visitQualifiedColTypeWithPosition(self, ctx:SqlBaseParser.QualifiedColTypeWithPositionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#colDefinitionDescriptorWithPosition.
    def visitColDefinitionDescriptorWithPosition(self, ctx:SqlBaseParser.ColDefinitionDescriptorWithPositionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#defaultExpression.
    def visitDefaultExpression(self, ctx:SqlBaseParser.DefaultExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#colTypeList.
    def visitColTypeList(self, ctx:SqlBaseParser.ColTypeListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#colType.
    def visitColType(self, ctx:SqlBaseParser.ColTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#createOrReplaceTableColTypeList.
    def visitCreateOrReplaceTableColTypeList(self, ctx:SqlBaseParser.CreateOrReplaceTableColTypeListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#createOrReplaceTableColType.
    def visitCreateOrReplaceTableColType(self, ctx:SqlBaseParser.CreateOrReplaceTableColTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#colDefinitionOption.
    def visitColDefinitionOption(self, ctx:SqlBaseParser.ColDefinitionOptionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#generationExpression.
    def visitGenerationExpression(self, ctx:SqlBaseParser.GenerationExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#complexColTypeList.
    def visitComplexColTypeList(self, ctx:SqlBaseParser.ComplexColTypeListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#complexColType.
    def visitComplexColType(self, ctx:SqlBaseParser.ComplexColTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#whenClause.
    def visitWhenClause(self, ctx:SqlBaseParser.WhenClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#windowClause.
    def visitWindowClause(self, ctx:SqlBaseParser.WindowClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#namedWindow.
    def visitNamedWindow(self, ctx:SqlBaseParser.NamedWindowContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#windowRef.
    def visitWindowRef(self, ctx:SqlBaseParser.WindowRefContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#windowDef.
    def visitWindowDef(self, ctx:SqlBaseParser.WindowDefContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#windowFrame.
    def visitWindowFrame(self, ctx:SqlBaseParser.WindowFrameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#frameBound.
    def visitFrameBound(self, ctx:SqlBaseParser.FrameBoundContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#qualifiedNameList.
    def visitQualifiedNameList(self, ctx:SqlBaseParser.QualifiedNameListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#functionName.
    def visitFunctionName(self, ctx:SqlBaseParser.FunctionNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#qualifiedName.
    def visitQualifiedName(self, ctx:SqlBaseParser.QualifiedNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#errorCapturingIdentifier.
    def visitErrorCapturingIdentifier(self, ctx:SqlBaseParser.ErrorCapturingIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#errorIdent.
    def visitErrorIdent(self, ctx:SqlBaseParser.ErrorIdentContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#realIdent.
    def visitRealIdent(self, ctx:SqlBaseParser.RealIdentContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#identifier.
    def visitIdentifier(self, ctx:SqlBaseParser.IdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#unquotedIdentifier.
    def visitUnquotedIdentifier(self, ctx:SqlBaseParser.UnquotedIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#quotedIdentifierAlternative.
    def visitQuotedIdentifierAlternative(self, ctx:SqlBaseParser.QuotedIdentifierAlternativeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#quotedIdentifier.
    def visitQuotedIdentifier(self, ctx:SqlBaseParser.QuotedIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#backQuotedIdentifier.
    def visitBackQuotedIdentifier(self, ctx:SqlBaseParser.BackQuotedIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#exponentLiteral.
    def visitExponentLiteral(self, ctx:SqlBaseParser.ExponentLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#decimalLiteral.
    def visitDecimalLiteral(self, ctx:SqlBaseParser.DecimalLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#legacyDecimalLiteral.
    def visitLegacyDecimalLiteral(self, ctx:SqlBaseParser.LegacyDecimalLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#integerLiteral.
    def visitIntegerLiteral(self, ctx:SqlBaseParser.IntegerLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#bigIntLiteral.
    def visitBigIntLiteral(self, ctx:SqlBaseParser.BigIntLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#smallIntLiteral.
    def visitSmallIntLiteral(self, ctx:SqlBaseParser.SmallIntLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#tinyIntLiteral.
    def visitTinyIntLiteral(self, ctx:SqlBaseParser.TinyIntLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#doubleLiteral.
    def visitDoubleLiteral(self, ctx:SqlBaseParser.DoubleLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#floatLiteral.
    def visitFloatLiteral(self, ctx:SqlBaseParser.FloatLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#bigDecimalLiteral.
    def visitBigDecimalLiteral(self, ctx:SqlBaseParser.BigDecimalLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#alterColumnAction.
    def visitAlterColumnAction(self, ctx:SqlBaseParser.AlterColumnActionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#stringLit.
    def visitStringLit(self, ctx:SqlBaseParser.StringLitContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#comment.
    def visitComment(self, ctx:SqlBaseParser.CommentContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#version.
    def visitVersion(self, ctx:SqlBaseParser.VersionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#ansiNonReserved.
    def visitAnsiNonReserved(self, ctx:SqlBaseParser.AnsiNonReservedContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#strictNonReserved.
    def visitStrictNonReserved(self, ctx:SqlBaseParser.StrictNonReservedContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#nonReserved.
    def visitNonReserved(self, ctx:SqlBaseParser.NonReservedContext):
        return self.visitChildren(ctx)



del SqlBaseParser