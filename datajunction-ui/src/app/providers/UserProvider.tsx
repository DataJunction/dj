import React, {
  createContext,
  useContext,
  useState,
  useEffect,
  useMemo,
  useCallback,
} from 'react';
import DJClientContext from './djclient';

interface User {
  id?: number;
  username?: string;
  email?: string;
  name?: string;
  last_viewed_notifications_at?: string | null;
}

interface UserContextType {
  currentUser: User | null;
  loading: boolean;
  error: Error | null;
  refetchUser: () => Promise<void>;
}

const UserContext = createContext<UserContextType>({
  currentUser: null,
  loading: true,
  error: null,
  refetchUser: async () => {},
});

export function UserProvider({ children }: { children: React.ReactNode }) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [currentUser, setCurrentUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchUser = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);
      const user = await djClient.whoami();
      setCurrentUser(user);
    } catch (err) {
      setError(err instanceof Error ? err : new Error('Failed to fetch user'));
      console.error('Error fetching user:', err);
    } finally {
      setLoading(false);
    }
  }, [djClient]);

  useEffect(() => {
    fetchUser();
  }, [fetchUser]);

  const value = useMemo(
    () => ({
      currentUser,
      loading,
      error,
      refetchUser: fetchUser,
    }),
    [currentUser, loading, error, fetchUser],
  );

  return <UserContext.Provider value={value}>{children}</UserContext.Provider>;
}

export function useCurrentUser() {
  const context = useContext(UserContext);
  if (context === undefined) {
    throw new Error('useCurrentUser must be used within a UserProvider');
  }
  return context;
}

export default UserContext;
