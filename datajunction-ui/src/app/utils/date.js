/**
 * Format a date string as relative time (e.g., "2h ago", "3d ago")
 */
export const formatRelativeTime = dateString => {
  const date = new Date(dateString);
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffMins = Math.floor(diffMs / 60000);
  const diffHours = Math.floor(diffMs / 3600000);
  const diffDays = Math.floor(diffMs / 86400000);

  if (diffMins < 1) return 'just now';
  if (diffMins < 60) return `${diffMins}m ago`;
  if (diffHours < 24) return `${diffHours}h ago`;
  if (diffDays < 7) return `${diffDays}d ago`;
  return date.toLocaleDateString();
};

/**
 * Get a date group label for grouping items (Today, Yesterday, This Week, etc.)
 */
export const getDateGroup = dateString => {
  const date = new Date(dateString);
  const now = new Date();

  // Reset times to compare dates only
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const yesterday = new Date(today);
  yesterday.setDate(yesterday.getDate() - 1);
  const thisWeekStart = new Date(today);
  thisWeekStart.setDate(thisWeekStart.getDate() - today.getDay());
  const lastWeekStart = new Date(thisWeekStart);
  lastWeekStart.setDate(lastWeekStart.getDate() - 7);

  const dateOnly = new Date(
    date.getFullYear(),
    date.getMonth(),
    date.getDate(),
  );

  if (dateOnly >= today) return 'Today';
  if (dateOnly >= yesterday) return 'Yesterday';
  if (dateOnly >= thisWeekStart) return 'This Week';
  if (dateOnly >= lastWeekStart) return 'Last Week';
  return 'Older';
};

/**
 * Group items by date using getDateGroup
 */
export const groupByDate = (items, dateField = 'created_at') => {
  const groups = {};
  const order = ['Today', 'Yesterday', 'This Week', 'Last Week', 'Older'];

  items.forEach(item => {
    const group = getDateGroup(item[dateField]);
    if (!groups[group]) groups[group] = [];
    groups[group].push(item);
  });

  // Return in order
  return order
    .filter(g => groups[g]?.length > 0)
    .map(g => ({ label: g, items: groups[g] }));
};
