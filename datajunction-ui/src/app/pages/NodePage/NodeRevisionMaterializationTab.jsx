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

  // For inactive-only versions, render with oval styling.
  //
  // A bare button, as `Tab` renders below: the `.col` and nav-tabs wrappers this sat
  // in carried `padding: 1.5rem` plus their own tab chrome, so one version chip
  // became a 24px-padded block sitting out of line with its neighbours.
  if (isOnlyInactive && showInactive) {
    return (
      <button
        key={version}
        id={version}
        className={
          selectedRevisionTab === version ? 'dj-tab dj-tab--active' : 'dj-tab'
        }
        tabIndex="0"
        onClick={onClickRevisionTab(version)}
        aria-label={tabName}
        aria-hidden="false"
        style={{
          borderRadius: '12px',
          backgroundColor: '#f5f5f5',
          border: '1px solid #ddd',
        }}
      >
        {tabName}
      </button>
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
