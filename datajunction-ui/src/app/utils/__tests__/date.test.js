import { formatRelativeTime, getDateGroup, groupByDate } from '../date';

describe('date utilities', () => {
  describe('formatRelativeTime', () => {
    it('returns "just now" for times less than 1 minute ago', () => {
      const now = new Date();
      expect(formatRelativeTime(now.toISOString())).toBe('just now');

      const thirtySecondsAgo = new Date(Date.now() - 30000);
      expect(formatRelativeTime(thirtySecondsAgo.toISOString())).toBe(
        'just now',
      );
    });

    it('returns minutes ago for times less than 1 hour ago', () => {
      const fiveMinutesAgo = new Date(Date.now() - 5 * 60000);
      expect(formatRelativeTime(fiveMinutesAgo.toISOString())).toBe('5m ago');

      const thirtyMinutesAgo = new Date(Date.now() - 30 * 60000);
      expect(formatRelativeTime(thirtyMinutesAgo.toISOString())).toBe(
        '30m ago',
      );

      const fiftyNineMinutesAgo = new Date(Date.now() - 59 * 60000);
      expect(formatRelativeTime(fiftyNineMinutesAgo.toISOString())).toBe(
        '59m ago',
      );
    });

    it('returns hours ago for times less than 24 hours ago', () => {
      const oneHourAgo = new Date(Date.now() - 1 * 3600000);
      expect(formatRelativeTime(oneHourAgo.toISOString())).toBe('1h ago');

      const twelveHoursAgo = new Date(Date.now() - 12 * 3600000);
      expect(formatRelativeTime(twelveHoursAgo.toISOString())).toBe('12h ago');

      const twentyThreeHoursAgo = new Date(Date.now() - 23 * 3600000);
      expect(formatRelativeTime(twentyThreeHoursAgo.toISOString())).toBe(
        '23h ago',
      );
    });

    it('returns days ago for times less than 7 days ago', () => {
      const oneDayAgo = new Date(Date.now() - 1 * 86400000);
      expect(formatRelativeTime(oneDayAgo.toISOString())).toBe('1d ago');

      const threeDaysAgo = new Date(Date.now() - 3 * 86400000);
      expect(formatRelativeTime(threeDaysAgo.toISOString())).toBe('3d ago');

      const sixDaysAgo = new Date(Date.now() - 6 * 86400000);
      expect(formatRelativeTime(sixDaysAgo.toISOString())).toBe('6d ago');
    });

    it('returns formatted date for times 7 or more days ago', () => {
      const sevenDaysAgo = new Date(Date.now() - 7 * 86400000);
      const result = formatRelativeTime(sevenDaysAgo.toISOString());
      // Should return a locale date string, not "Xd ago"
      expect(result).not.toContain('d ago');
      expect(result).toMatch(/\d/); // Should contain numbers (date)
    });
  });

  describe('getDateGroup', () => {
    it('returns "Today" for dates from today', () => {
      const now = new Date();
      expect(getDateGroup(now.toISOString())).toBe('Today');

      // Earlier today (midnight)
      const midnight = new Date();
      midnight.setHours(0, 0, 0, 0);
      expect(getDateGroup(midnight.toISOString())).toBe('Today');
    });

    it('returns "Yesterday" for dates from yesterday', () => {
      const yesterday = new Date();
      yesterday.setDate(yesterday.getDate() - 1);
      expect(getDateGroup(yesterday.toISOString())).toBe('Yesterday');
    });

    it('returns "This Week" for dates from this week (but not today or yesterday)', () => {
      const now = new Date();
      const dayOfWeek = now.getDay();

      // Only test if we're not on Sunday (0) or Monday (1)
      // because "This Week" starts on Sunday
      if (dayOfWeek >= 2) {
        const twoDaysAgo = new Date();
        twoDaysAgo.setDate(twoDaysAgo.getDate() - 2);
        expect(getDateGroup(twoDaysAgo.toISOString())).toBe('This Week');
      }
    });

    it('returns "Last Week" for dates from last week', () => {
      const now = new Date();
      const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
      const dayOfWeek = today.getDay();

      // Calculate start of this week (Sunday)
      const thisWeekStart = new Date(today);
      thisWeekStart.setDate(thisWeekStart.getDate() - dayOfWeek);

      // Last week is 1-7 days before this week's start
      const lastWeekDate = new Date(thisWeekStart);
      lastWeekDate.setDate(lastWeekDate.getDate() - 3); // Middle of last week

      expect(getDateGroup(lastWeekDate.toISOString())).toBe('Last Week');
    });

    it('returns "Older" for dates older than last week', () => {
      const threeWeeksAgo = new Date();
      threeWeeksAgo.setDate(threeWeeksAgo.getDate() - 21);
      expect(getDateGroup(threeWeeksAgo.toISOString())).toBe('Older');

      const monthAgo = new Date();
      monthAgo.setMonth(monthAgo.getMonth() - 1);
      expect(getDateGroup(monthAgo.toISOString())).toBe('Older');
    });
  });

  describe('groupByDate', () => {
    it('groups items by their date', () => {
      const now = new Date();
      const yesterday = new Date();
      yesterday.setDate(yesterday.getDate() - 1);
      const lastMonth = new Date();
      lastMonth.setMonth(lastMonth.getMonth() - 1);

      const items = [
        { id: 1, created_at: now.toISOString() },
        { id: 2, created_at: now.toISOString() },
        { id: 3, created_at: yesterday.toISOString() },
        { id: 4, created_at: lastMonth.toISOString() },
      ];

      const result = groupByDate(items);

      expect(result).toHaveLength(3); // Today, Yesterday, Older

      const todayGroup = result.find(g => g.label === 'Today');
      expect(todayGroup.items).toHaveLength(2);
      expect(todayGroup.items.map(i => i.id)).toEqual([1, 2]);

      const yesterdayGroup = result.find(g => g.label === 'Yesterday');
      expect(yesterdayGroup.items).toHaveLength(1);
      expect(yesterdayGroup.items[0].id).toBe(3);

      const olderGroup = result.find(g => g.label === 'Older');
      expect(olderGroup.items).toHaveLength(1);
      expect(olderGroup.items[0].id).toBe(4);
    });

    it('returns groups in correct order', () => {
      const now = new Date();
      const yesterday = new Date();
      yesterday.setDate(yesterday.getDate() - 1);
      const lastMonth = new Date();
      lastMonth.setMonth(lastMonth.getMonth() - 1);

      // Items in random order
      const items = [
        { id: 1, created_at: lastMonth.toISOString() },
        { id: 2, created_at: now.toISOString() },
        { id: 3, created_at: yesterday.toISOString() },
      ];

      const result = groupByDate(items);
      const labels = result.map(g => g.label);

      // Should be in order: Today, Yesterday, Older
      expect(labels).toEqual(['Today', 'Yesterday', 'Older']);
    });

    it('uses custom date field when specified', () => {
      const now = new Date();
      const items = [{ id: 1, updated_at: now.toISOString() }];

      const result = groupByDate(items, 'updated_at');

      expect(result).toHaveLength(1);
      expect(result[0].label).toBe('Today');
    });

    it('returns empty array for empty input', () => {
      const result = groupByDate([]);
      expect(result).toEqual([]);
    });

    it('only includes groups that have items', () => {
      const now = new Date();
      const items = [{ id: 1, created_at: now.toISOString() }];

      const result = groupByDate(items);

      expect(result).toHaveLength(1);
      expect(result[0].label).toBe('Today');
    });
  });
});
