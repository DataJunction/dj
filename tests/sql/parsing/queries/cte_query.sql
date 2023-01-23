WITH
  cteReports
  AS
  (
    SELECT EmployeeID, FirstName, LastName, ManagerID
    FROM Employees
    WHERE ManagerID IS NULL
  )

SELECT
  FirstName + ' ' + LastName AS FullName,
  EmpLevel,
  (SELECT FirstName + ' ' + LastName FROM Employees
    WHERE EmployeeID = cteReports.MgrID) AS Manager
FROM cteReports
