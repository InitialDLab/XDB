create index on customer using btree (c_mktsegment);
create index on orders using btree (o_custkey);
create index on lineitem using btree (l_orderkey);

create index on lineitem using btree (l_returnflag);

create index on nation using btree (n_name);
create index on supplier using btree (s_nationkey);
create index on customer using btree (c_nationkey);
create index on lineitem using btree (l_orderkey);
create index on lineitem using btree (l_suppkey);

create index on lineitem using btree (l_shipdate);

