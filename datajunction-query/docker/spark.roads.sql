CREATE DATABASE roads;

CREATE TABLE roads.repair_type (
    repair_type_id int,
    repair_type_name string,
    contractor_id string
);
INSERT INTO roads.repair_type VALUES
(1, 'Asphalt Overlay', 'Asphalt overlays restore roads to a smooth condition. This resurfacing uses the deteriorating asphalt as a base for which the new layer is added on top of, instead of tearing up the worsening one.'),
(2, 'Patching', 'Patching is the process of filling potholes or excavated areas in the asphalt pavement. Quick repair of potholes or other pavement disintegration helps control further deterioration and expensive repair of the pavement. Without timely patching, water can enter the sub-grade and cause larger and more serious pavement failures.'),
(3, 'Reshaping', 'This is necessary when a road surface it too damaged to be smoothed. Using a grader blade and scarifying if necessary, you rework the gravel sub-base to eliminate large potholes and rebuild a flattened crown.'),
(4, 'Slab Replacement', 'This refers to replacing sections of paved roads. It is a good option for when slabs are chipped, cracked, or uneven, and mitigates the need to replace the entire road when just a small section is damaged.'),
(5, 'Smoothing', 'This is when you lightly rework the gravel of a road without digging in too far to the sub-base. Typically, a motor grader is used in this operation with an attached blade. Smoothing is done when the road has minor damage or is just worn down a bit from use.'),
(6, 'Reconstruction', 'When roads have deteriorated to a point that it is no longer cost-effective to maintain, the entire street or road needs to be rebuilt. Typically, this work is done in phases to limit traffic restrictions. As part of reconstruction, the street may be realigned to improve safety or operations, grading may be changed to improve storm water flow, underground utilities may be added, upgraded or relocated, traffic signals and street lights may be relocated, and street trees and pedestrian ramps may be added.');

CREATE TABLE roads.municipality (
    municipality_id string,
    contact_name string,
    contact_title string,
    local_region string,
    state_id int
);
INSERT INTO roads.municipality VALUES
('New York', 'Alexander Wilkinson', 'Assistant City Clerk', 'Manhattan', 33),
('Los Angeles', 'Hugh Moser', 'Administrative Assistant', 'Santa Monica',5 ),
('Chicago', 'Phillip Bradshaw', 'Director of Community Engagement', 'West Ridge', 14),
('Houston', 'Leo Ackerman', 'Municipal Roads Specialist', 'The Woodlands', 44),
('Phoenix', 'Jessie Paul', 'Director of Finance and Administration', 'Old Town Scottsdale', 3),
('Philadelphia', 'Willie Chaney', 'Municipal Manager', 'Center City', 39),
('San Antonio', 'Chester Lyon', 'Treasurer', 'Alamo Heights', 44),
('San Diego', 'Ralph Helms', 'Senior Electrical Project Manager', 'Del Mar', 5),
('Dallas', 'Virgil Craft', 'Assistant Assessor (Town/Municipality)', 'Deep Ellum', 44),
('San Jose', 'Charles Carney', 'Municipal Accounting Manager', 'Santana Row', 5);

CREATE TABLE roads.hard_hats (
    hard_hat_id int,
    last_name string,
    first_name string,
    title string,
    birth_date date,
    hire_date date,
    address string,
    city string,
    state string,
    postal_code string,
    country string,
    manager int,
    contractor_id int
);
INSERT INTO roads.hard_hats VALUES
(1, 'Brian', 'Perkins', 'Construction Laborer', cast('1978-11-28' as date), cast('2009-02-06' as date), '4 Jennings Ave.', 'Jersey City', 'NJ', '37421', 'USA', 9, 1),
(2, 'Nicholas', 'Massey', 'Carpenter', cast('1993-02-19' as date), cast('2003-04-14' as date), '9373 Southampton Street', 'Middletown', 'CT', '27292', 'USA', 9, 1),
(3, 'Cathy', 'Best', 'Framer', cast('1994-08-30' as date), cast('1990-07-02' as date), '4 Hillside Street', 'Billerica', 'MA', '13440', 'USA', 9, 2),
(4, 'Melanie', 'Stafford', 'Construction Manager', cast('1966-03-19' as date), cast('2003-02-02' as date), '77 Studebaker Lane', 'Southampton', 'PA', '71730', 'USA', 9, 2),
(5, 'Donna', 'Riley', 'Pre-construction Manager', cast('1983-03-14' as date), cast('2012-01-13' as date), '82 Taylor Drive', 'Southgate', 'MI', '33125', 'USA', 9, 4),
(6, 'Alfred', 'Clarke', 'Construction Superintendent', cast('1979-01-12' as date), cast('2013-10-17' as date), '7729 Catherine Street', 'Powder Springs', 'GA', '42001', 'USA', 9, 2),
(7, 'William', 'Boone', 'Construction Laborer', cast('1970-02-28' as date), cast('2013-01-02' as date), '1 Border St.', 'Niagara Falls', 'NY', '14304', 'USA', 9, 4),
(8, 'Luka', 'Henderson', 'Construction Laborer', cast('1988-12-09' as date), cast('2013-03-05' as date), '794 S. Chapel Ave.', 'Phoenix', 'AZ', '85021', 'USA', 9, 1),
(9, 'Patrick', 'Ziegler', 'Construction Laborer', cast('1976-11-27' as date), cast('2020-11-15' as date), '321 Gainsway Circle', 'Muskogee', 'OK', '74403', 'USA', 9, 3);

CREATE TABLE roads.hard_hat_state (
    hard_hat_id int,
    state_id int
);
INSERT INTO roads.hard_hat_state VALUES
(1, 2),
(2, 32),
(3, 28),
(4, 12),
(5, 5),
(6, 3),
(7, 16),
(8, 32),
(9, 41);

CREATE TABLE roads.repair_order_details (
    repair_order_id int,
    repair_type_id int,
    price real NOT NULL,
    quantity int,
    discount real NOT NULL
);
INSERT INTO roads.repair_order_details VALUES
(10001, 1, 63708, 1, 0.05),
(10002, 4, 67253, 1, 0.05),
(10003, 2, 66808, 1, 0.05),
(10004, 4, 18497, 1, 0.05),
(10005, 7, 76463, 1, 0.05),
(10006, 4, 87858, 1, 0.05),
(10007, 1, 63918, 1, 0.05),
(10008, 6, 21083, 1, 0.05),
(10009, 3, 74555, 1, 0.05),
(10010, 5, 27222, 1, 0.05),
(10011, 5, 73600, 1, 0.05),
(10012, 3, 54901, 1, 0.01),
(10013, 5, 51594, 1, 0.01),
(10014, 1, 65114, 1, 0.01),
(10015, 1, 48919, 1, 0.01),
(10016, 3, 70418, 1, 0.01),
(10017, 1, 29684, 1, 0.01),
(10018, 2, 62928, 1, 0.01),
(10019, 2, 97916, 1, 0.01),
(10020, 5, 44120, 1, 0.01),
(10021, 1, 53374, 1, 0.01),
(10022, 2, 87289, 1, 0.01),
(10023, 2, 92366, 1, 0.01),
(10024, 2, 47857, 1, 0.01),
(10025, 1, 68745, 1, 0.01);

CREATE TABLE roads.repair_orders (
    repair_order_id int,
    municipality_id string,
    hard_hat_id int,
    order_date date,
    required_date date,
    dispatched_date date,
    dispatcher_id int
);
INSERT INTO roads.repair_orders VALUES
(10001, 'New York', 1, cast('2007-07-04' as date), cast('2009-07-18' as date), cast('2007-12-01' as date), 3),
(10002, 'New York', 3, cast('2007-07-05' as date), cast('2009-08-28' as date), cast('2007-12-01' as date), 1),
(10003, 'New York', 5, cast('2007-07-08' as date), cast('2009-08-12' as date), cast('2007-12-01' as date), 2),
(10004, 'Dallas', 1, cast('2007-07-08' as date), cast('2009-08-01' as date), cast('2007-12-01' as date), 1),
(10005, 'San Antonio', 8, cast('2007-07-09' as date), cast('2009-08-01' as date), cast('2007-12-01' as date), 2),
(10006, 'New York', 3, cast('2007-07-10' as date), cast('2009-08-01' as date), cast('2007-12-01' as date), 2),
(10007, 'Philadelphia', 4, cast('2007-04-21' as date), cast('2009-08-08' as date), cast('2007-12-01' as date), 2),
(10008, 'Philadelphia', 5, cast('2007-04-22' as date), cast('2009-08-09' as date), cast('2007-12-01' as date), 3),
(10009, 'Philadelphia', 3, cast('2007-04-25' as date), cast('2009-08-12' as date), cast('2007-12-01' as date), 2),
(10010, 'Philadelphia', 4, cast('2007-04-26' as date), cast('2009-08-13' as date), cast('2007-12-01' as date), 3),
(10011, 'Philadelphia', 4, cast('2007-04-27' as date), cast('2009-08-14' as date), cast('2007-12-01' as date), 1),
(10012, 'Philadelphia', 8, cast('2007-04-28' as date), cast('2009-08-15' as date), cast('2007-12-01' as date), 3),
(10013, 'Philadelphia', 4, cast('2007-04-29' as date), cast('2009-08-16' as date), cast('2007-12-01' as date), 1),
(10014, 'Philadelphia', 6, cast('2007-04-29' as date), cast('2009-08-16' as date), cast('2007-12-01' as date), 2),
(10015, 'Philadelphia', 2, cast('2007-04-12' as date), cast('2009-08-19' as date), cast('2007-12-01' as date), 3),
(10016, 'Philadelphia', 9, cast('2007-04-13' as date), cast('2009-08-20' as date), cast('2007-12-01' as date), 3),
(10017, 'Philadelphia', 2, cast('2007-04-14' as date), cast('2009-08-21' as date), cast('2007-12-01' as date), 3),
(10018, 'Philadelphia', 6, cast('2007-04-15' as date), cast('2009-08-22' as date), cast('2007-12-01' as date), 1),
(10019, 'Philadelphia', 5, cast('2007-05-16' as date), cast('2009-09-06' as date), cast('2007-12-01' as date), 3),
(10020, 'Philadelphia', 1, cast('2007-05-19' as date), cast('2009-08-26' as date), cast('2007-12-01' as date), 1),
(10021, 'Philadelphia', 7, cast('2007-05-10' as date), cast('2009-08-27' as date), cast('2007-12-01' as date), 3),
(10022, 'Philadelphia', 5, cast('2007-05-11' as date), cast('2009-08-14' as date), cast('2007-12-01' as date), 1),
(10023, 'Philadelphia', 1, cast('2007-05-11' as date), cast('2009-08-29' as date), cast('2007-12-01' as date), 1),
(10024, 'Philadelphia', 5, cast('2007-05-11' as date), cast('2009-08-29' as date), cast('2007-12-01' as date), 2),
(10025, 'Philadelphia', 6, cast('2007-05-12' as date), cast('2009-08-30' as date), cast('2007-12-01' as date), 2);

CREATE TABLE roads.dispatchers (
    dispatcher_id int,
    company_name string,
    phone string
);
INSERT INTO roads.dispatchers VALUES
(1, 'Pothole Pete', '(111) 111-1111'),
(2, 'Asphalts R Us', '(222) 222-2222'),
(3, 'Federal Roads Group', '(333) 333-3333'),
(4, 'Local Patchers', '1-800-888-8888'),
(5, 'Gravel INC', '1-800-000-0000'),
(6, 'DJ Developers', '1-111-111-1111');

CREATE TABLE roads.contractors (
    contractor_id int,
    company_name string,
    contact_name string,
    contact_title string,
    address string,
    city string,
    state string,
    postal_code string,
    country string,
    phone string
);
INSERT INTO roads.contractors VALUES
(1, 'You Need Em We Find Em', 'Max Potter', 'Assistant Director', '4 Plumb Branch Lane', 'Goshen', 'IN', '46526', 'USA', '(111) 111-1111'),
(2, 'Call Forwarding', 'Sylvester English', 'Administrator', '9650 Mill Lane', 'Raeford', 'NC', '28376', 'USA', '(222) 222-2222'),
(3, 'The Connect', 'Paul Raymond', 'Administrator', '7587 Myrtle Ave.', 'Chaska', 'MN', '55318', 'USA', '(333) 333-3333');

CREATE TABLE roads.us_region (
    us_region_id int,
    us_region_description string
);
INSERT INTO roads.us_region VALUES
(1, 'Eastern'),
(2, 'Western'),
(3, 'Northern'),
(4, 'Southern');

CREATE TABLE roads.us_states (
    state_id int,
    state_name string,
    state_abbr string,
    state_region string
);
INSERT INTO roads.us_states VALUES
(1, 'Alabama', 'AL', 'Southern'),
(2, 'Alaska', 'AK', 'Northern'),
(3, 'Arizona', 'AZ', 'Western'),
(4, 'Arkansas', 'AR', 'Southern'),
(5, 'California', 'CA', 'Western'),
(6, 'Colorado', 'CO', 'Western'),
(7, 'Connecticut', 'CT', 'Eastern'),
(8, 'Delaware', 'DE', 'Eastern'),
(9, 'District of Columbia', 'DC', 'Eastern'),
(10, 'Florida', 'FL', 'Southern'),
(11, 'Georgia', 'GA', 'Southern'),
(12, 'Hawaii', 'HI', 'Western'),
(13, 'Idaho', 'ID', 'Western'),
(14, 'Illinois', 'IL', 'Western'),
(15, 'Indiana', 'IN', 'Western'),
(16, 'Iowa', 'IO', 'Western'),
(17, 'Kansas', 'KS', 'Western'),
(18, 'Kentucky', 'KY', 'Southern'),
(19, 'Louisiana', 'LA', 'Southern'),
(20, 'Maine', 'ME', 'Northern'),
(21, 'Maryland', 'MD', 'Eastern'),
(22, 'Massachusetts', 'MA', 'Northern'),
(23, 'Michigan', 'MI', 'Northern'),
(24, 'Minnesota', 'MN', 'Northern'),
(25, 'Mississippi', 'MS', 'Southern'),
(26, 'Missouri', 'MO', 'Southern'),
(27, 'Montana', 'MT', 'Western'),
(28, 'Nebraska', 'NE', 'Western'),
(29, 'Nevada', 'NV', 'Western'),
(30, 'New Hampshire', 'NH', 'Eastern'),
(31, 'New Jersey', 'NJ', 'Eastern'),
(32, 'New Mexico', 'NM', 'Western'),
(33, 'New York', 'NY', 'Eastern'),
(34, 'North Carolina', 'NC', 'Eastern'),
(35, 'North Dakota', 'ND', 'Western'),
(36, 'Ohio', 'OH', 'Western'),
(37, 'Oklahoma', 'OK', 'Western'),
(38, 'Oregon', 'OR', 'Western'),
(39, 'Pennsylvania', 'PA', 'Eastern'),
(40, 'Rhode Island', 'RI', 'Eastern'),
(41, 'South Carolina', 'SC', 'Eastern'),
(42, 'South Dakota', 'SD', 'Western'),
(43, 'Tennessee', 'TN', 'Western'),
(44, 'Texas', 'TX', 'Western'),
(45, 'Utah', 'UT', 'Western'),
(46, 'Vermont', 'VT', 'Eastern'),
(47, 'Virginia', 'VA', 'Eastern'),
(48, 'Washington', 'WA', 'Western'),
(49, 'West Virginia', 'WV', 'Southern'),
(50, 'Wisconsin', 'WI', 'Western'),
(51, 'Wyoming', 'WY', 'Western');

CREATE TABLE roads.municipality_municipality_type (
    municipality_id string,
    municipality_type_id string
);
INSERT INTO roads.municipality_municipality_type VALUES
('New York', 'A'),
('Los Angeles', 'B'),
('Chicago', 'B'),
('Houston', 'A'),
('Phoenix', 'A'),
('Philadelphia', 'B'),
('San Antonio', 'A'),
('San Diego', 'B'),
('Dallas', 'A'),
('San Jose', 'B');

CREATE TABLE roads.municipality_type (
    municipality_type_id string,
    municipality_type_desc string
);
INSERT INTO roads.municipality_type VALUES
('A', 'Primary'),
('A', 'Secondary');
