CREATE SCHEMA roads;

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
    state_id int,
    phone string
);
INSERT INTO roads.municipality VALUES
('New York', 'Alexander Wilkinson', 'Assistant City Clerk', 'Manhattan', 33, '202-291-2922'),
('Los Angeles', 'Hugh Moser', 'Administrative Assistant', 'Santa Monica', 5, '808-211-2323'),
('Chicago', 'Phillip Bradshaw', 'Director of Community Engagement', 'West Ridge', 14, '425-132-3421'),
('Houston', 'Leo Ackerman', 'Municipal Roads Specialist', 'The Woodlands', 44, '413-435-8641'),
('Phoenix', 'Jessie Paul', 'Director of Finance and Administration', 'Old Town Scottsdale', 3, '321-425-5427'),
('Philadelphia', 'Willie Chaney', 'Municipal Manager', 'Center City', 39, '212-213-5361'),
('San Antonio', 'Chester Lyon', 'Treasurer', 'Alamo Heights', 44, '252-216-6938'),
('San Diego', 'Ralph Helms', 'Senior Electrical Project Manager', 'Del Mar', 5, '491-813-2417'),
('Dallas', 'Virgil Craft', 'Assistant Assessor (Town/Municipality)', 'Deep Ellum', 44, '414-563-7894'),
('San Jose', 'Charles Carney', 'Municipal Accounting Manager', 'Santana Row', 5, '408-313-0698');

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

CREATE SCHEMA campaigns;

CREATE TABLE campaigns.campaign_views (
    campaign_id int,
    campaign_name string,
    created_at timestamp,
    views int
);

INSERT INTO campaigns.campaign_views VALUES
(1, 'Clean Up Your Yard Marketing', epoch_ms(1526290800000), 120),
(2, 'Summer Sale Campaign', epoch_ms(1456472400000), 500),
(3, 'New Product Launch', epoch_ms(1341392400000), 250),
(4, 'Holiday Special Offers', epoch_ms(1451600400000), 800),
(5, 'Spring Cleaning Deals', epoch_ms(1472667600000), 300),
(6, 'Back to School Promotion', epoch_ms(1293848400000), 150),
(7, 'Winter Clearance Sale', epoch_ms(1512123600000), 900),
(8, 'Outdoor Adventure Campaign', epoch_ms(1288779600000), 400),
(9, 'Health and Wellness Expo', epoch_ms(1335795600000), 200),
(10, 'Summer Vacation Deals', epoch_ms(1462069200000), 600),
(11, 'Home Renovation Offers', epoch_ms(1396314000000), 350),
(12, 'Fashion Show Sponsorship', epoch_ms(1248478800000), 750),
(13, 'Charity Fundraising Drive', epoch_ms(1363563600000), 420),
(14, 'Tech Gadgets Showcase', epoch_ms(1433106000000), 230),
(15, 'Gourmet Food Festival', epoch_ms(1308032400000), 150),
(16, 'Music Concert Ticket Sales', epoch_ms(1380584400000), 550),
(17, 'Book Fair and Author Meet', epoch_ms(1262274000000), 180),
(18, 'Fitness Challenge Event', epoch_ms(1502053200000), 670),
(19, 'Pet Adoption Awareness', epoch_ms(1319638800000), 290),
(20, 'Art Exhibition Opening', epoch_ms(1409422800000), 390),
(21, 'Wedding Planning Expo', epoch_ms(1274245200000), 420),
(22, 'Sports Equipment Sale', epoch_ms(1419987600000), 780),
(23, 'Tech Startup Conference', epoch_ms(1370302800000), 210),
(24, 'Environmental Awareness', epoch_ms(1328053200000), 840),
(25, 'Travel and Adventure Expo', epoch_ms(1366832400000), 350),
(26, 'Automobile Showroom Launch', epoch_ms(1425162000000), 420),
(27, 'Film Festival Promotion', epoch_ms(1356997200000), 590),
(28, 'Summer Camp Enrollment', epoch_ms(1388778000000), 240),
(29, 'Online Shopping Festival', epoch_ms(1251776400000), 430),
(30, 'Healthcare Symposium', epoch_ms(1443642000000), 980);

CREATE TABLE campaigns.email (
    campaign_id int,
    email_address string,
    email_id int,
    last_contacted timestamp,
    views int
);

INSERT INTO campaigns.email VALUES
(1, 'mark@fakedomain.com', 1, to_timestamp(1451606400000), 2),
(1, 'john@fakedomain.com', 2, to_timestamp(1454284800000), 7),
(1, 'isaiah@fakedomain.com', 3, to_timestamp(1456790400000), 10),
(1, 'ruby@fakedomain.com', 4, to_timestamp(1459468800000), 5),
(1, 'oliver@fakedomain.com', 5, to_timestamp(1462060800000), 9),
(1, 'emma@fakedomain.com', 6, to_timestamp(1464739200000), 12),
(1, 'liam@fakedomain.com', 7, to_timestamp(1467331200000), 4),
(1, 'ava@fakedomain.com', 8, to_timestamp(1470009600000), 11),
(1, 'noah@fakedomain.com', 9, to_timestamp(1472688000000), 7),
(1, 'isabella@fakedomain.com', 10, to_timestamp(1475280000000), 3),
(2, 'sophia@fakedomain.com', 1, to_timestamp(1477958400000), 8),
(2, 'mason@fakedomain.com', 2, to_timestamp(1480550400000), 6),
(2, 'camila@fakedomain.com', 3, to_timestamp(1483228800000), 11),
(2, 'henry@fakedomain.com', 4, to_timestamp(1485907200000), 13),
(2, 'mia@fakedomain.com', 5, to_timestamp(1488326400000), 6),
(2, 'ethan@fakedomain.com', 6, to_timestamp(1491004800000), 9),
(2, 'lucas@fakedomain.com', 7, to_timestamp(1493596800000), 5),
(2, 'harper@fakedomain.com', 8, to_timestamp(1496275200000), 14),
(2, 'alexander@fakedomain.com', 9, to_timestamp(1498867200000), 12),
(2, 'abigail@fakedomain.com', 10, to_timestamp(1501545600000), 2),
(3, 'james@fakedomain.com', 1, to_timestamp(1504224000000), 7),
(3, 'amelia@fakedomain.com', 2, to_timestamp(1506816000000), 4),
(3, 'benjamin@fakedomain.com', 3, to_timestamp(1509494400000), 10),
(3, 'evelyn@fakedomain.com', 4, to_timestamp(1512086400000), 6),
(3, 'michael@fakedomain.com', 5, to_timestamp(1514764800000), 3),
(3, 'charlotte@fakedomain.com', 6, to_timestamp(1517443200000), 8),
(3, 'daniel@fakedomain.com', 7, to_timestamp(1520035200000), 12),
(3, 'harper@fakedomain.com', 8, to_timestamp(1522713600000), 9),
(3, 'lucas@fakedomain.com', 9, to_timestamp(1525305600000), 5),
(3, 'isabella@fakedomain.com', 10, to_timestamp(1527984000000), 11);

CREATE TABLE campaigns.sms (
    campaign_id int,
    phone_number string,
    message string,
    last_contacted timestamp,
);

INSERT INTO campaigns.sms VALUES
(11, '(215) 111-1111', 'Click here to redeem 20% off!: http://smalllink', epoch_ms(1459468800000)),
(12, '(215) 222-2222', 'Interested in new lawn equipment?: http://smalllink', epoch_ms(1459468800001)),
(12, '(215) 333-3333', 'These sales will not last!: http://smalllink', epoch_ms(1459468800002)),
(13, '(215) 444-4444', 'Fall is here, enjoy half-off!: http://smalllink', epoch_ms(1459468800003)),
(14, '(215) 555-5555', 'Get the best deals today!: http://smalllink', epoch_ms(1459468800004)),
(15, '(215) 666-6666', 'Limited time offer - 30% off!: http://smalllink', epoch_ms(1459468800005)),
(16, '(215) 777-7777', 'Do not miss out on our sale!: http://smalllink', epoch_ms(1459468800006)),
(17, '(215) 888-8888', 'Exclusive discount for you!: http://smalllink', epoch_ms(1459468800007)),
(18, '(215) 999-9999', 'Shop now and save big!: http://smalllink', epoch_ms(1459468800008)),
(19, '(215) 000-0000', 'Huge clearance sale - up to 50% off!: http://smalllink', epoch_ms(1459468800009)),
(10, '(215) 123-4567', 'New arrivals with special discounts!: http://smalllink', epoch_ms(1459468800010)),
(11, '(215) 234-5678', 'Save money with our promo codes!: http://smalllink', epoch_ms(1459468800011)),
(12, '(215) 345-6789', 'Enjoy free shipping on all orders!: http://smalllink', epoch_ms(1459468800012)),
(13, '(215) 456-7890', 'Limited stock available - act fast!: http://smalllink', epoch_ms(1459468800013)),
(14, '(215) 567-8901', 'Upgrade your home with our deals!: http://smalllink', epoch_ms(1459468800014)),
(15, '(215) 678-9012', 'Special discount for loyal customers!: http://smalllink', epoch_ms(1459468800015)),
(16, '(215) 789-0123', 'Do not miss our summer sale!: http://smalllink', epoch_ms(1459468800016)),
(17, '(215) 890-1234', 'Exclusive offer - limited time only!: http://smalllink', epoch_ms(1459468800017)),
(18, '(215) 901-2345', 'Shop now and get a free gift!: http://smalllink', epoch_ms(1459468800018)),
(19, '(215) 012-3456', 'Big discounts on popular brands!: http://smalllink', epoch_ms(1459468800019)),
(10, '(215) 987-6543', 'Save up to 70% off on selected items!: http://smalllink', epoch_ms(1459468800020)),
(11, '(215) 876-5432', 'Limited time offer - buy one, get one free!: http://smalllink', epoch_ms(1459468800021)),
(12, '(215) 765-4321', 'Great deals for your next vacation!: http://smalllink', epoch_ms(1459468800022)),
(13, '(215) 654-3210', 'Get ready for the holiday season with our discounts!: http://smalllink', epoch_ms(1459468800023)),
(14, '(215) 543-2109', 'Save on electronics and gadgets!: http://smalllink', epoch_ms(1459468800024)),
(15, '(215) 432-1098', 'Limited stock available - you do not want to miss out!: http://smalllink', epoch_ms(1459468800025)),
(16, '(215) 321-0987', 'Shop now and enjoy free returns!: http://smalllink', epoch_ms(1459468800026)),
(17, '(215) 210-9876', 'Exclusive discount for online orders!: http://smalllink', epoch_ms(1459468800027)),
(18, '(215) 109-8765', 'Upgrade your wardrobe with our sale!: http://smalllink', epoch_ms(1459468800028)),
(19, '(215) 098-7654', 'Will you miss our clearance sale??: http://smalllink', epoch_ms(1459468800029)),
(10, '(215) 987-6543', 'Get the best deals for your home!: http://smalllink', epoch_ms(1459468800030)),
(12, '(215) 222-2222', 'New collection now available!: http://smalllink', epoch_ms(1459468800002)),
(13, '(215) 333-3333', 'Limited stock - dont miss out!: http://smalllink', epoch_ms(1459468800003)),
(14, '(215) 444-4444', 'Free shipping on all orders!: http://smalllink', epoch_ms(1459468800004)),
(15, '(215) 555-5555', 'Sign up for exclusive offers!: http://smalllink', epoch_ms(1459468800005)),
(16, '(215) 666-6666', 'Get ready for summer with our new arrivals!: http://smalllink', epoch_ms(1459468800006)),
(17, '(215) 777-7777', 'Limited time sale - up to 60% off!: http://smalllink', epoch_ms(1459468800007)),
(10, '(215) 111-2222', 'Shop now and receive a gift card!: http://smalllink', epoch_ms(1459468800010)),
(11, '(215) 222-3333', 'Final clearance - last chance to save!: http://smalllink', epoch_ms(1459468800011)),
(12, '(215) 333-4444', 'Get the latest fashion trends at discounted prices!: http://smalllink', epoch_ms(1459468800012)),
(13, '(215) 444-5555', 'Upgrade your electronics with our special offers!: http://smalllink', epoch_ms(1459468800013)),
(14, '(215) 555-6666', 'Big savings on home appliances!: http://smalllink', epoch_ms(1459468800014)),
(15, '(215) 666-7777', 'Limited stock - shop now before its gone!: http://smalllink', epoch_ms(1459468800015)),
(16, '(215) 777-8888', 'Get the best deals on beauty products!: http://smalllink', epoch_ms(1459468800016)),
(11, '(215) 012-3456', 'Dont miss our summer sale!: http://smalllink', epoch_ms(1459468800021)),
(12, '(215) 123-4567', 'Exclusive offer for our loyal customers!: http://smalllink', epoch_ms(1459468800022)),
(13, '(215) 234-5678', 'Shop now and enjoy free returns!: http://smalllink', epoch_ms(1459468800023)),
(14, '(215) 345-6789', 'Upgrade your gaming setup with our deals!: http://smalllink', epoch_ms(1459468800024)),
(15, '(215) 456-7890', 'Save on outdoor essentials for your next adventure!: http://smalllink', epoch_ms(1459468800025)),
(16, '(215) 567-8901', 'Limited time offer - buy one, get one free!: http://smalllink', epoch_ms(1459468800026)),
(19, '(215) 098-7654', 'Discover the latest tech gadgets at discounted prices!: http://smalllink', epoch_ms(1459468800029)),
(12, '(215) 876-5432', 'Get the perfect gift for your loved ones!: http://smalllink', epoch_ms(1459468800031));

CREATE TABLE campaigns.commercial (
    campaign_id int,
    station string,
    last_played timestamp,
);

INSERT INTO campaigns.commercial VALUES
(23, 'Montgomery Oldies Station', epoch_ms(1688879124599)),
(21, 'Lithonia Jazz & Blues', epoch_ms(1688879124599)),
(20, 'Manchester 90s R&B', epoch_ms(1688879124599)),
(22, 'Los Angeles Rock Hits', epoch_ms(1688879124599)),
(23, 'Chicago Pop Mix', epoch_ms(1688879124599)),
(24, 'Houston Country Station', epoch_ms(1688879124599)),
(25, 'New York Hip Hop', epoch_ms(1688879124599)),
(26, 'Seattle Alternative Rock', epoch_ms(1688879124599)),
(27, 'Miami Latin Beats', epoch_ms(1688879124599)),
(28, 'Denver Indie Folk', epoch_ms(1688879124599)),
(29, 'Boston Classical', epoch_ms(1688879124599)),
(20, 'San Francisco Electronic', epoch_ms(1688879124599)),
(21, 'Philadelphia R&B Hits', epoch_ms(1688879124599)),
(22, 'Dallas Pop Punk', epoch_ms(1688879124599)),
(23, 'Atlanta Gospel', epoch_ms(1688879124599)),
(24, 'Las Vegas Hard Rock', epoch_ms(1688879124599)),
(25, 'Phoenix Country Hits', epoch_ms(1688879124599)),
(26, 'Portland Alternative', epoch_ms(1688879124599)),
(27, 'Austin Indie Rock', epoch_ms(1688879124599)),
(28, 'Nashville Country Classics', epoch_ms(1688879124599)),
(29, 'San Diego Surf Rock', epoch_ms(1688879124599)),
(20, 'Minneapolis Folk', epoch_ms(1688879124599)),
(21, 'Detroit Motown', epoch_ms(1688879124599)),
(22, 'Baltimore Jazz Lounge', epoch_ms(1688879124599)),
(23, 'Kansas City Blues', epoch_ms(1688879124599)),
(24, 'St. Louis Smooth Jazz', epoch_ms(1688879124599)),
(25, 'Cleveland Classic Rock', epoch_ms(1688879124599)),
(26, 'Pittsburgh Metal', epoch_ms(1688879124599)),
(27, 'Charlotte Pop Hits', epoch_ms(1688879124599)),
(28, 'Raleigh-Durham Indie Pop', epoch_ms(1688879124599)),
(29, 'Tampa Bay Reggae', epoch_ms(1688879124599));

CREATE SCHEMA games;

CREATE TABLE games.titles (
    game_id int,
    game_name string,
    num_distinct_players int,
    release_date timestamp,
    platform string,
    genre string,
    publisher_id int,
    developer_id int,
    average_rating float,
    online_mode boolean,
    total_sales int,
    active_monthly int,
    dlcs int
);

INSERT INTO games.titles (
    game_id,
    game_name,
    num_distinct_players,
    release_date,
    platform,
    genre,
    publisher_id,
    developer_id,
    average_rating,
    online_mode,
    total_sales,
    active_monthly,
    dlcs
)
VALUES
    (1, 'Battle of the Chinchillas: Furry Fury', 1000, '2022-01-01 00:00:00', 'PS5', 'Action', 1, 928, 4.5, true, 1000000, 5000, 3),
    (2, 'Grand Theft Toaster: Carb City Chronicles', 500, '2021-06-15 00:00:00', 'Xbox Series X', 'RPG', 1, 948, 4.2, false, 750000, 2500, 2),
    (3, 'Super Slime Soccer: Gooey Goalkeepers', 2000, '2020-11-30 00:00:00', 'Nintendo Switch', 'Sports', 2, 937, 4.8, true, 500000, 3000, 4),
    (4, 'Dance Dance Avocado: Guacamole Groove', 1500, '2022-08-20 00:00:00', 'PC', 'Rhythm', 22, 987, 4.6, true, 250000, 2000, 1),
    (5, 'Zombie Zookeeper: Undead Menagerie', 800, '2023-02-10 00:00:00', 'PS4', 'Strategy', 13, 902, 4.4, false, 300000, 1500, 3),
    (6, 'Squirrel Simulator: Nutty Adventure', 3000, '2021-03-05 00:00:00', 'Xbox One', 'Simulation', 15, 928, 4.2, true, 400000, 2500, 2),
    (7, 'Crash Test Dummies: Wacky Collision', 1200, '2022-05-15 00:00:00', 'PC', 'Action', 12, 928, 4.7, false, 150000, 1000, 1),
    (8, 'Pizza Delivery Panic: Cheesy Chaos', 1000, '2023-01-30 00:00:00', 'Nintendo Switch', 'Arcade', 8, 928, 4.3, true, 200000, 1800, 2),
    (9, 'Alien Abduction Academy: Extraterrestrial Education', 2500, '2020-09-05 00:00:00', 'PS5', 'Adventure', 7, 987, 4.5, false, 800000, 3500, 3),
    (10, 'Crazy Cat Circus: Meow Mayhem', 700, '2022-07-25 00:00:00', 'Xbox Series X', 'Puzzle', 18, 987, 4.1, true, 100000, 1200, 1),
    (11, 'Robot Rampage: Mechanical Mayhem', 1800, '2021-04-12 00:00:00', 'PC', 'Action', 4, 987, 4.6, true, 450000, 2200, 2),
    (12, 'Super Spy Squirrels: Nutty Espionage', 900, '2023-03-18 00:00:00', 'PS4', 'Stealth', 10, 934, 4.4, false, 400000, 1800, 3),
    (13, 'Banana Blaster: Fruit Frenzy', 2800, '2021-02-08 00:00:00', 'Xbox One', 'Shooter', 10, 934, 4.3, true, 700000, 3000, 2),
    (14, 'Penguin Paradise: Antarctic Adventure', 1100, '2022-04-05 00:00:00', 'PC', 'Simulation', 20, 902, 4.7, false, 200000, 1200, 1),
    (15, 'Unicorn Universe: Rainbow Realm', 500, '2023-01-15 00:00:00', 'Nintendo Switch', 'Adventure', 1, 902, 4.2, true, 150000, 900, 2),
    (16, 'Spaghetti Showdown: Saucy Shootout', 2100, '2020-10-20 00:00:00', 'PS5', 'Action', 4, 902, 4.4, true, 550000, 2500, 3),
    (17, 'Bubblegum Bandits: Sticky Heist', 800, '2022-06-10 00:00:00', 'Xbox Series X', 'Stealth', 3, 902, 4.1, false, 180000, 800, 1),
    (18, 'Safari Slingshot: Wild Wildlife', 1300, '2021-03-01 00:00:00', 'PC', 'Arcade', 3, 987, 4.6, true, 300000, 1500, 2),
    (19, 'Monster Mop: Cleaning Catastrophe', 600, '2022-12-20 00:00:00', 'Nintendo Switch', 'Puzzle', 17, 934, 4.3, false, 120000, 700, 1),
    (20, 'Galactic Golf: Space Swing', 1900, '2020-08-15 00:00:00', 'PS4', 'Sports', 5, 921, 4.5, true, 400000, 2000, 3),
    (21, 'Funky Farm Friends: Groovy Gardening', 900, '2023-02-05 00:00:00', 'Xbox One', 'Simulation', 21, 921, 4.3, true, 250000, 1300, 2),
    (22, 'Cake Crusaders: Sugary Siege', 1400, '2021-01-25 00:00:00', 'PC', 'Strategy', 23, 902, 4.7, false, 180000, 900, 1),
    (23, 'Ninja Narwhal: Aquatic Assassin', 1000, '2022-03-15 00:00:00', 'Nintendo Switch', 'Action', 23, 901, 4.2, true, 150000, 1000, 2);

CREATE TABLE games.publishers (
    publisher_id int PRIMARY KEY,
    publisher_name string
);

INSERT INTO games.publishers (publisher_id, publisher_name)
VALUES
    (1, 'Wacky Game Studios'),
    (2, 'Silly Monkey Games'),
    (3, 'Laughing Unicorn Interactive'),
    (4, 'Crazy Cat Games'),
    (5, 'Quirky Penguin Productions'),
    (6, 'Absurd Antelope Studios'),
    (7, 'Hilarious Hedgehog Games'),
    (8, 'Goofy Giraffe Studios'),
    (9, 'Whimsical Walrus Entertainment'),
    (10, 'Zany Zebra Games'),
    (11, 'Ridiculous Rabbit Studios'),
    (12, 'Funny Fox Interactive'),
    (13, 'Surreal Snake Games'),
    (14, 'Bizarre Bat Studios'),
    (15, 'Madcap Moose Productions'),
    (16, 'Cuckoo Clock Games'),
    (17, 'Lunatic Llama Studios'),
    (18, 'Silly Goose Games'),
    (19, 'Bonkers Beaver Interactive'),
    (20, 'Witty Walrus Games');

CREATE TABLE games.developers (
    developer_id int,
    developer_name string,
    num_games_developed int
);

INSERT INTO games.developers (developer_id, developer_name, num_games_developed)
VALUES
    (928, 'CrazyCodr', 10),
    (948, 'PixelPirate', 5),
    (937, 'CodeNinja', 3),
    (987, 'GameWizard', 8),
    (902, 'ByteBender', 15),
    (934, 'CodeJester', 4),
    (902, 'MadGenius', 12),
    (921, 'GameGuru', 9),
    (901, 'ScriptMage', 6);

CREATE SCHEMA accounting;
CREATE TABLE accounting.payment_type_table (
    id int,
    payment_type_name string,
    payment_type_classification string
);

INSERT INTO accounting.payment_type_table (id, payment_type_name, payment_type_classification)
VALUES
    (1, 'VISA', 'CARD'),
    (2, 'MASTERCARD', 'CARD');


CREATE TABLE accounting.revenue (
    payment_id int,
    payment_amount float,
    payment_type int,
    customer_id int,
    account_type string
);

INSERT INTO accounting.revenue (payment_id, payment_amount, payment_type, customer_id, account_type)
VALUES
    (1, 25.5, 1, 2, 'ACTIVE'),
    (2, 12.5, 2, 2, 'INACTIVE'),
    (3, 89, 1, 3, 'ACTIVE'),
    (4, 1293.2, 2, 2, 'ACTIVE'),
    (5, 23, 1, 4, 'INACTIVE'),
    (6, 398.13, 2, 3, 'ACTIVE'),
    (7, 239.7, 2, 4, 'ACTIVE'),;
