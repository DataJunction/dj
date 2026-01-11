import { formatRelativeTime, getDateGroup, groupByDate } from '../date';

describe('date utilities', () => {
  describe('formatRelativeTime', () => {
    it('returns "just now" for times less than a minute ago', () => {
      const now = new Date();
      expect(formatRelativeTime(now.toISOString())).toBe('just now');
    });

    it('returns minutes for times less than an hour ago', () => {
      const date = new Date();
      date.setMinutes(date.getMinutes() - 30);
      expect(formatRelativeTime(date.toISOString())).toBe('30m ago');
    });

    it('returns hours for times less than a day ago', () => {
      const date = new Date();
      date.setHours(date.getHours() - 5);
      expect(formatRelativeTime(date.toISOString())).toBe('5h ago');
    });

    it('returns days for times less than a week ago', () => {
      const date = new Date();
      date.setDate(date.getDate() - 3);
      expect(formatRelativeTime(date.toISOString())).toBe('3d ago');
    });

    it('returns formatted date for times more than a week ago', () => {
      const date = new Date();
      date.setDate(date.getDate() - 10);
      const result = formatRelativeTime(date.toISOString());
      expect(result).toMatch(/\d{1,2}\/\d{1,2}\/\d{4}/);
    });
  });

  describe('getDateGroup', () => {
    it('returns "Today" for today\'s date', () => {
      const now = new Date();
      expect(getDateGroup(now.toISOString())).toBe('Today');
    });

    it('returns "Yesterday" for yesterday\'s date', () => {
      const yesterday = new Date();
      yesterday.setDate(yesterday.getDate() - 1);
      expect(getDateGroup(yesterday.toISOString())).toBe('Yesterday');
    });

    it('returns "Last Week" for dates from last week', () => {
      const now = new Date();
      const dayOfWeek = now.getDay();
      const lastWeek = new Date();
      lastWeek.setDate(lastWeek.getDate() - dayOfWeek - 3); // Go to last week
      expect(getDateGroup(lastWeek.toISOString())).toBe('Last Week');
    });

    it('returns "Older" for dates more than two weeks ago', () => {
      const oldDate = new Date();
      oldDate.setDate(oldDate.getDate() - 20);
      expect(getDateGroup(oldDate.toISOString())).toBe('Older');
    });
  });

  describe('getDateGroup with mocked time', () => {
    // Test "This Week" branch with a fixed date (Wednesday, Jan 15, 2025)
    beforeEach(() => {
      jest.useFakeTimers();
      jest.setSystemTime(new Date('2025-01-15T12:00:00Z'));
    });

    afterEach(() => {
      jest.useRealTimers();
    });

    it('returns "This Week" for dates earlier this week', () => {
      // Jan 15, 2025 is Wednesday. Week starts Sunday Jan 12.
      // A date from Monday Jan 13 should be "This Week"
      const thisWeekDate = new Date('2025-01-13T12:00:00Z');
      expect(getDateGroup(thisWeekDate.toISOString())).toBe('This Week');
    });

    it('returns "This Week" for Sunday of the current week', () => {
      // Sunday Jan 12, 2025 is the start of this week
      const sundayThisWeek = new Date('2025-01-12T12:00:00Z');
      expect(getDateGroup(sundayThisWeek.toISOString())).toBe('This Week');
    });
  });

  describe('groupByDate', () => {
    it('groups items by date', () => {
      const now = new Date();
      const yesterday = new Date();
      yesterday.setDate(yesterday.getDate() - 1);

      const items = [
        { id: 1, created_at: now.toISOString() },
        { id: 2, created_at: yesterday.toISOString() },
      ];

      const groups = groupByDate(items);
      expect(groups.length).toBeGreaterThan(0);
      expect(groups[0].label).toBe('Today');
    });

    it('returns empty array for empty input', () => {
      const groups = groupByDate([]);
      expect(groups).toEqual([]);
    });

    it('uses custom date field', () => {
      const now = new Date();
      const items = [{ id: 1, updated_at: now.toISOString() }];

      const groups = groupByDate(items, 'updated_at');
      expect(groups.length).toBe(1);
      expect(groups[0].label).toBe('Today');
    });
  });
});
