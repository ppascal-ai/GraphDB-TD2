// Constraints: ensure stable ids on core node labels
CREATE CONSTRAINT customer_id_unique IF NOT EXISTS
FOR (c:Customer)
REQUIRE c.id IS UNIQUE;

CREATE CONSTRAINT product_id_unique IF NOT EXISTS
FOR (p:Product)
REQUIRE p.id IS UNIQUE;

CREATE CONSTRAINT order_id_unique IF NOT EXISTS
FOR (o:Order)
REQUIRE o.id IS UNIQUE;

CREATE CONSTRAINT category_id_unique IF NOT EXISTS
FOR (cat:Category)
REQUIRE cat.id IS UNIQUE;

// Helpful indexes for common match patterns
CREATE INDEX product_category_idx IF NOT EXISTS
FOR (p:Product)
ON (p.category_id);

CREATE INDEX customer_email_idx IF NOT EXISTS
FOR (c:Customer)
ON (c.email);

CREATE INDEX product_name_idx IF NOT EXISTS
FOR (p:Product)
ON (p.name);


