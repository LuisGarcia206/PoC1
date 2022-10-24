CREATE TABLE db1_poc1.departments (
  id INTEGER OPTIONS(description="Id of the job"),
  department STRING OPTIONS(description="Name of the job")
);
CREATE TABLE db1_poc1.jobs (
  id INTEGER OPTIONS(description="Id of the department"),
  job STRING OPTIONS(description="Name of the department")
);
CREATE TABLE db1_poc1.hired_employees (
  id INTEGER OPTIONS(description="Id of the employee"),
  name STRING OPTIONS(description="Name and surname of the employee"),
  datetime STRING OPTIONS(description="Hire datetime in ISO format"),
  department_id INTEGER OPTIONS(description="Id of the department"),
  job_id INTEGER OPTIONS(description="Id of the job")
);