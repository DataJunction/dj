import '../../styles/loading.css';

export default function LoadingIcon({ centered = true }) {
  const content = (
    <div className="lds-ring">
      <div></div>
      <div></div>
      <div></div>
      <div></div>
    </div>
  );

  return centered ? <center>{content}</center> : content;
}
