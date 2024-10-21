import { useContext } from 'react';
import { Outlet } from 'react-router-dom';
import DJLogo from '../../icons/DJLogo';
import { Helmet } from 'react-helmet-async';
import DJClientContext from '../../providers/djclient';
import Search from '../../components/Search';

// Define the type for the docs sites
type DocsSites = {
  [key: string]: string;
};

// Default docs sites if REACT_APP_DOCS_SITES is not defined
const defaultDocsSites: DocsSites = {
  'Open-Source': 'https://www.datajunction.io/',
};

// Parse the JSON map from the environment variable or use the default
const docsSites: DocsSites = process.env.REACT_APP_DOCS_SITES
  ? (JSON.parse(process.env.REACT_APP_DOCS_SITES as string) as DocsSites)
  : defaultDocsSites;

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
                  <div className="dropdown">
                    <a
                      className="btn btn-link dropdown-toggle"
                      href="#"
                      id="docsDropdown"
                      role="button"
                      aria-expanded="false"
                    >
                      Docs
                    </a>
                    <ul
                      className="dropdown-menu"
                      aria-labelledby="docsDropdown"
                    >
                      {Object.entries(docsSites).map(([key, value]) => (
                        <li key={key}>
                          <a
                            className="dropdown-item"
                            href={value}
                            target="_blank"
                            rel="noreferrer"
                          >
                            {key}
                          </a>
                        </li>
                      ))}
                    </ul>
                  </div>
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
