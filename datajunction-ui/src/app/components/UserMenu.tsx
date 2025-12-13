import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../providers/djclient';
import { useCurrentUser } from '../providers/UserProvider';

interface User {
  id: number;
  username: string;
  email: string;
  name?: string;
}

// Extract initials from user's name or username
const getInitials = (user: User | null): string => {
  if (!user) return '?';
  if (user.name) {
    return user.name
      .split(' ')
      .map(n => n[0])
      .join('')
      .toUpperCase()
      .slice(0, 2);
  }
  return user.username.slice(0, 2).toUpperCase();
};

interface UserMenuProps {
  onDropdownToggle?: (isOpen: boolean) => void;
  forceClose?: boolean;
}

export default function UserMenu({
  onDropdownToggle,
  forceClose,
}: UserMenuProps) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const { currentUser } = useCurrentUser();
  const [showDropdown, setShowDropdown] = useState(false);

  // Close when forceClose becomes true
  useEffect(() => {
    if (forceClose && showDropdown) {
      setShowDropdown(false);
    }
  }, [forceClose, showDropdown]);

  // Close dropdown when clicking outside
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      const target = event.target as HTMLElement;
      if (!target.closest('.user-menu-dropdown')) {
        setShowDropdown(false);
        onDropdownToggle?.(false);
      }
    };
    document.addEventListener('click', handleClickOutside);
    return () => document.removeEventListener('click', handleClickOutside);
  }, [onDropdownToggle]);

  const handleToggle = (e: React.MouseEvent) => {
    e.stopPropagation();
    const willOpen = !showDropdown;
    setShowDropdown(willOpen);
    onDropdownToggle?.(willOpen);
  };

  const handleLogout = async () => {
    await djClient.logout();
    window.location.reload();
  };

  return (
    <div className="nav-dropdown user-menu-dropdown">
      <button className="avatar-button" onClick={handleToggle}>
        {getInitials(currentUser as User | null)}
      </button>
      {showDropdown && (
        <div className="nav-dropdown-menu">
          <div className="dropdown-header">
            {currentUser?.username || 'User'}
          </div>
          <hr className="dropdown-divider" />
          <a className="dropdown-item" href="/settings">
            Settings
          </a>
          <a className="dropdown-item" href="/" onClick={handleLogout}>
            Logout
          </a>
        </div>
      )}
    </div>
  );
}
