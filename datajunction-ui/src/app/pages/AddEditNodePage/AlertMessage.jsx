import AlertIcon from '../../icons/AlertIcon';

export const AlertMessage = ({ message }) => {
  return (
    <div className="message alert">
      <AlertIcon />
      {message}
    </div>
  );
};
