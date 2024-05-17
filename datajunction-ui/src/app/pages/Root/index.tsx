import { useContext } from 'react';
import { Outlet } from 'react-router-dom';
import DJLogo from '../../icons/DJLogo';
import { Helmet } from 'react-helmet-async';
import DJClientContext from '../../providers/djclient';
import Search from '../../components/Search';

export function Root() {
  const djClient = useContext(DJClientContext).DataJunctionAPI;

  const handleLogout = async () => {
    await djClient.logout();
    window.location.reload();
  };
  return (
    <>
      <Helmet>
        <title>DataJunction</title>
        <meta
          name="description"
          content="DataJunction Metrics Platform Webapp"
        />
      </Helmet>
      <div className="container d-flex align-items-center justify-content-between">
        <div className="header">
          <div className="logo">
            <a
              href={'/'}
              style={{
                textTransform: 'none',
                textDecoration: 'none',
                color: '#000',
              }}
            >
              <h2>
                <DJLogo />
                Data<b>Junction</b>
              </h2>
            </a>
          </div>
          <Search />
          <div className="menu">
            <div className="menu-item here menu-here-bg menu-lg-down-accordion me-0 me-lg-2 fw-semibold">
              <span className="menu-link">
                <span className="menu-title">
                  <a href="/">Explore</a>
                </span>
              </span>
              <span className="menu-link">
                <span className="menu-title">
                  <a href="/sql">SQL</a>
                </span>
              </span>
              <span className="menu-link">
                <span className="menu-title">
                  <a
                    href="https://www.datajunction.io"
                    target="_blank"
                    rel="noreferrer"
                  >
                    Docs
                  </a>
                </span>
              </span>
            </div>
          </div>
        </div>
        {process.env.REACT_DISABLE_AUTH === 'true' ? (
          ''
        ) : (
          <span className="menu-link">
            <span className="menu-title">
              <a href={'/'} onClick={handleLogout}>
                Logout
              </a>
            </span>
          </span>
        )}
      </div>
      <Outlet />
    </>
  );
}
