import '../../styles/loading.css';

export default function LoadingIcon() {
  return (
    <center>
      <div className="lds-ring">
        <div></div>
        <div></div>
        <div></div>
        <div></div>
      </div>
    </center>
  );
}
