/**
 * jeefo-sqlite3-orm
 * https://github.com/je3f0o/jeefo-sqlite3-orm
 *
 * Copyright (c) 2014 je3f0o
 * Licensed under the MIT license.
 */

"use strict";

var Fields    = require("./fields"),
	WhereNode = require("./where_node"),
	p;

// Private methods
function build_where_nodes (query, groups, order_fields, operator) {
	var where_str = "";

	groups.forEach(function (group, index) {
		var where_query = group.build();

		if (index > 0) {
			where_str += " " + operator + " ";
		}

		where_str   += where_query.str;
		query.values = query.values.concat(where_query.values);

		if (where_query.sort) {
			order_fields.push(where_query.sort);
		}
	});

	return where_str;
}

function build_wheres (query, $this, to_set_total) {
	var where_str = "";

	if ($this.groups.length || $this.or_groups.length) {
		where_str = " WHERE ";
	}

	if ($this.groups.length) {
		where_str += build_where_nodes(query, $this.groups, $this.order_fields, "AND")
	}

	if ($this.or_groups.length) {
		if (where_str !== " WHERE ") {
			where_str += " OR ";
		}
		where_str += build_where_nodes(query, $this.or_groups, $this.order_fields, "OR")
	}

	query.str += where_str;
	if (to_set_total) {
		query.total += where_str;
	}
}

function is_int (n) { return n === +n && n === (n | 0); }

module.exports = function () { return this.reset(); };
p = module.exports.prototype;

p.reset = function () {
	this.has_created_time = true;
	this.has_updated_time = true;
	this.select           = "*";
	this.fields           = {};
	this.groups           = [];
	this.or_groups        = [];
	this.orders           = [];
	this.order_fields     = [];
	this._limit = { offset : 0 };

	return this;
};

p.limit = function (limit) {
	if (limit !== null && typeof limit === "object") {
		this._limit.offset = is_int(limit.offset) ? limit.offset : 0;
		this._limit.max    = is_int(limit.max)    ? limit.max    : 0;
	} else {
		this._limit.max = is_int(limit) ? limit : 0;
	}

	this._limit.offset = this._limit.offset >= 0 ? this._limit.offset : 0;
	this._limit.max    = this._limit.max    >= 0 ? this._limit.max    : 0;

	return this;
};

p.order_by = function (orders) {
	Object.keys(orders).forEach(function (key) {
		this.orders.push("`" + key + "` " + orders[key].toUpperCase());
	}, this);
};

p.set_select = function (field) {
	if (field instanceof Array) {
		this.select = field.join(", ");
	} else if (typeof field === "string") {
		this.select = "`" + field + "`";
	}

	return this;
};

p.where = function (wheres) {
	if (wheres.$limit !== void 0) {
		this.limit(wheres.$limit);
		delete wheres.$limit;
	}

	if (wheres.$order_by) {
		this.order_by(wheres.$order_by);
		delete wheres.$order_by;
	}

	if (wheres.$select) {
		this.set_select(wheres.$select);
		delete wheres.$select;
	}

	if (wheres.$groups) {
		wheres.$groups.forEach(function (group) {
			this.groups.push(new WhereNode(group));
		}, this);

		delete wheres.$groups;
	}

	if (wheres.$or_groups) {
		wheres.$or_groups.forEach(function (group) {
			this.or_groups.push(new WhereNode(group));
		}, this);

		delete wheres.$or_groups;
	}

	if (Object.keys(wheres).length > 0) {
		this.groups.unshift(new WhereNode(wheres));
	}

	return this;
};

/**
 * Builders ---------------------
 */
p.build_select_query = function (table) {
	table = WhereNode.parse_table_name(table);
	
	var query = {
		str    : "SELECT " + this.select + " FROM " + table,
		values : []
	}, has_ordered;

	if (this.select.indexOf(",") >= 0) {
		query.total = "SELECT COUNT(*) AS `total` FROM " + table;
	} else {
		//SELECT FOUND_ROWS() // it was bad idea because it is slow !
		query.total = "SELECT COUNT(" + this.select + ") AS `total` FROM " + table;
	}

	build_wheres(query, this, true);
	query.total_values = query.values;

	this.order_fields.forEach(function (order_field, index) {
		if (index === 0) {
			query.str += " ORDER BY ";
			has_ordered = true;
		} else {
			query.str += ", ";
		}

		query.str   += "FIELD (" + order_field.key + ", " + order_field.place_holder + ")";
		query.values = query.values.concat(order_field.values);
	}, this);

	if (this.orders.length) {
		if (! has_ordered) {
			query.str += " ORDER BY ";
		}

		query.str += this.orders.join(", ");
	}
	if (this._limit.max !== void 0) {
		query.str += " LIMIT " + this._limit.offset + ", " + this._limit.max;
	}
	query.total += " LIMIT 1";

	return query;
};

p.build_insert_query = function (table, fields) {
	table  = WhereNode.parse_table_name(table);
	fields = new Fields(fields);

	var query = {
		str    : "INSERT INTO " + table,
		values : []
	},
	place_holders = [];

	var keys = fields.map(function (field) {
		place_holders.push("?");
		query.values.push(field.value);
		return field.key;
	});

	if (this.has_created_time) {
		keys.push("created_at");
		place_holders.push("NOW()");
	}
	if (this.has_updated_time) {
		keys.push("updated_at");
		place_holders.push("NOW()");
	}

	query.str += "(`" + keys.join("`, `") + "`) ";
	query.str += "VALUES(" + place_holders.join(", ") + ")";

	return query;
};

p.build_update_query = function (table, fields) {
	table  = WhereNode.parse_table_name(table);
	fields = new Fields(fields);

	var query = {
		str    : "UPDATE " + table + " SET ",
		values : []
	},
	place_holders = [];

	fields.forEach(function (field) {
		place_holders.push("`" + field.key + "` = ?");
		query.values.push(field.value);
	});
	if (this.has_updated_time) {
		place_holders.push("`updated_at` = NOW()");
	}

	query.str += place_holders.join(", ");
	build_wheres(query, this);

	return query;
};

p.build_delete_query = function (table) {
	table = WhereNode.parse_table_name(table);

	var query = {
		str    : "DELETE FROM " + table,
		values : []
	};

	build_wheres(query, this);

	return query;
};

if (require.main === module) {
	var sql = new module.exports("users");
	sql.where({
		firstname : "John",
		lastname : "Doe",
		email : {
			$not : "person@example.com"
		},
		age : {
			$lte : 15,
			$gte : 20
		},
		id : [55,56,57],
		$sort : true,
		$groups : [
			{
				date : {
					$not : "something"
				},
				love : "Sugi",
				person_to_meet : {
					$like : "%jeefo%"
				},
				id : [99,98,97],
				$sort : true,
				age : {
					$lt : 10,
					$gt : 20
				}
			}
		],
		$or_groups : [
			{
				type : { $not : "fuck" },
				tab : null
			},
			{
				type : { $not : "cool" }
			}
		]
	});

	var query = sql.build_select_query("test");
	console.log(query.str);
	console.log(query.values);
	console.log("-------------");
	console.log(query.total);
	console.log(query.total_values);

	console.log("=======================================");

	sql = new module.exports("users");
	sql.where({
		$select : {
			$distinct : "id"
		}
	});
	query = sql.build_select_query("test");

	console.log(query.str);
	console.log(query.values);
	console.log("-------------");
	console.log(query.total);
	console.log(query.total_values);
}
