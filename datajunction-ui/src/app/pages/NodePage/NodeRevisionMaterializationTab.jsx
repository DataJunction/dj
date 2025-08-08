import Tab from '../../components/Tab';

export default function NodeRevisionMaterializationTab({
  version,
  node,
  selectedRevisionTab,
  onClickRevisionTab,
  showInactive,
  versionHasOnlyInactive,
}) {
  const isCurrentVersion = version === node?.version;
  const tabName = isCurrentVersion ? `${version} (latest)` : version;
  const versionInfo = versionHasOnlyInactive[version];
  const isOnlyInactive =
    versionInfo && !versionInfo.hasActive && versionInfo.hasInactive;

  // For inactive-only versions, render with oval styling
  if (isOnlyInactive && showInactive) {
    return (
      <div
        key={version}
        className={selectedRevisionTab === version ? 'col active' : 'col'}
      >
        <div className="header-tabs nav-overflow nav nav-tabs">
          <div className="nav-item">
            <button
              id={version}
              className="nav-link"
              tabIndex="0"
              onClick={onClickRevisionTab(version)}
              aria-label={tabName}
              aria-hidden="false"
              style={{
                padding: '4px 8px',
                borderRadius: '12px',
                backgroundColor: '#f5f5f5',
                border: '1px solid #ddd',
                margin: '0 2px',
              }}
            >
              {tabName}
            </button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <Tab
      key={version}
      id={version}
      name={tabName}
      onClick={onClickRevisionTab(version)}
      selectedTab={selectedRevisionTab}
    />
  );
}
