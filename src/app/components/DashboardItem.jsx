import { Component } from 'react';

export default class DashboardItem extends Component {
  render() {
    const { label, value } = this.props;
    return (
      // <div className="col-xl col-md-6 col-12">
      //   <div className="card">
      //     <div className="card-body">
      //       <div className="align-items-center row">
      //         <div className="col"><h6 className="text-uppercase text-muted mb-2">{ label }</h6><span
      //           className="h2 mb-0">{ value }</span><span className="mt-n1 ms-2 badge bg-success-soft">+3.5%</span></div>
      //         <div className="col-auto">
      //           <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"
      //                stroke-linecap="round" stroke-linejoin="round" className="feather feather-dollar-sign text-muted">
      //             <g>
      //               <line x1="12" y1="1" x2="12" y2="23"></line>
      //               <path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"></path>
      //             </g>
      //           </svg>
      //         </div>
      //       </div>
      //     </div>
      //   </div>
      // </div>
      <div></div>
    );
  }
}
