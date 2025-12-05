import React from 'react';

/**
 * Displays user profile information including avatar, username, and email.
 */
export function ProfileSection({ user }) {
  const getInitials = () => {
    if (user?.name) {
      return user.name
        .split(' ')
        .map(n => n[0])
        .join('')
        .toUpperCase()
        .slice(0, 2);
    }
    return user?.username?.slice(0, 2).toUpperCase() || '?';
  };

  return (
    <section className="settings-section">
      <h2 className="settings-section-title">Profile</h2>
      <div className="settings-card">
        <div className="profile-info">
          <div className="profile-avatar">{getInitials()}</div>
          <div className="profile-details">
            <div className="profile-field">
              <label>Username</label>
              <span>{user?.username || '-'}</span>
            </div>
            <div className="profile-field">
              <label>Email</label>
              <span>{user?.email || '-'}</span>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

export default ProfileSection;
