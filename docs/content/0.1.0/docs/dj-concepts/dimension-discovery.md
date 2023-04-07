---
weight: 30
---

# Dimension Discovery

In data warehousing, dimensions are parts of the data model that play a huge role in making data understandable and intuitively composable. If
you want to learn more about your users, it's convenient to have a `users` dimension table with all of the attributes that belong
to each user. If your company is expanding into global markets, maintaining a `country` dimension table that keeps track of business-relevant
data for each individual country will come in handy. You can think of many more examples but the key role of dimensions in data warehousing
is to categorize, describe, and segment your event data.

A construction company may have a `repair_orders` table that contains all repair order requests. It's possible this table only has unique IDs
for the repair order, the municipality where the work is to be performed, and the dispatcher that will coordinate the repair. To provide meaningful
insights and reporting, you'll need to pull in more information from other dimension tables such as the requester's location, repair's location,
dispatcher's contact information, estimate price, and actual final price.

[Diagram of a simple repair_orders fact table with a few arrows pointing to the dimension ID fields]

---

## The Power of Dimension Nodes in DJ

At first glance, a dimension node in DJ looks very similar to transform nodes. It's a named node that includes a query which can select from one
or more other nodes. However, a dimension node is different in that it can be referenced by columns on any source node, transform node, and even
other dimension nodes. To understand that, let's look at a simple example of how a fact table is enriched with data from a dimension
table.

[Diagram of a simple left join from the `repair_orders` table to the `dispatchers` table]

The `repair_orders` table contains an ID of the dispatcher who's responsible for coordinating the work. In order to
pull in more information about the dispatcher, a join must be performed to the `dispatchers` dimension table which has a `dispatcher_id`
column that serves as its primary key. As you can see, this requires knowledge of the table's layout and the proper dimension table to join to.
It's not uncommon for a request for particular datasets to be "enriched" in this way to go through multiple channels until a data professional
most familiar with the data model performs this join and stores the result in a new table.

In DJ, you can pre-empt these kind of requests by tagging the column with information about which dimension node it is tied to. Instead of actually
joining to the `dispatchers` dimension node, you can label the `dispatcher_id` column on the `repair_orders` node with a
reference to the ID column on the `dispatchers` table. Now when someone makes a request asking DJ "What dimension attributes are available for
the metric `num_repair_orders`?", DJ will list all of the attribute columns from the `dispatchers` dimension node. If the user requests
the number of repair orders grouped by each dispatcher, including the dispatcher's contact information, DJ uses the column label to perform the
correct join and retrieve the extra attributes from the `dispatchers` table.

[Diagram showing DJ use the column dimension label to join to the dispatchers dimension node]

---

## Normalization & Denormalization Techniques

In data modeling, there are two analogous techniques to organizing tables. **Normalization** separates data into smaller tables and leans into linking
them through join keys. The benefits of this technique is that it reduces redundant data and provides a more logical layout. For example, instead of
including employee attributes such as name, phone number, and salary in many duplicated columns across multiple tables throughout your data model,
a normalized data model would keep all of that information in a single `employees` dimension table with only the employee IDs existing in other
tables. The downside of highly normalized data is that you pretty much always have to perform joins to enrich your event data beyond just IDs. In other
words, when you **do** need specific employee attributes beyond their unique ID, you always have to join to pull that information in.

The opposite technique is called **Denormalization** where data is stored in "wider" tables that already include many of the attributes of various
dimensions. This technique leans into redundantly including attributes of the dimension on the same table. The benefit of denormalization is that it
decreases the frequency with which joins need to be performed to pull in information about various dimensions. This means less work for the query engine
to perform and the cost of redundancy in turn leads to better performing queries.

DJ doesn't have an opinion on either data modeling technique. Both types of data models will work well and you can define metric and dimension nodes on
either data model type. That being said, DJ does a great job at easing the burden of knowing which join to do in a highly normalized data model.
Using dimension labels on columns, DJ abstracts away a complicated relationship model behind an intuitive API where data or SQL can be retrieved for
a set of metrics and dimensions.
