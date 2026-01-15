export default function Tab({ id, name, icon, onClick, selectedTab }) {
  const isActive = selectedTab === id;
  
  return (
    <button
      id={id}
      className={`underline-tab${isActive ? ' active' : ''}`}
      onClick={onClick}
      aria-label={name}
      aria-selected={isActive}
      role="tab"
    >
      {icon}
      {name}
    </button>
  );
}
