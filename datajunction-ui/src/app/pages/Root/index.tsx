import { Outlet } from 'react-router-dom';
import logo from './assets/dj-logo.png';
import { Helmet } from 'react-helmet-async';

export function Root() {
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
            <h2>
              <img src={logo} alt="DJ Logo" width="15%" />
              DataJunction
            </h2>
          </div>
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
      </div>
      <Outlet />
    </>
  );
}
