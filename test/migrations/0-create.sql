-- up-migration
CREATE TABLE person (
  id SERIAL PRIMARY KEY,
  name varchar(100) NOT NULL
);

-- down-migration
DROP TABLE person;
