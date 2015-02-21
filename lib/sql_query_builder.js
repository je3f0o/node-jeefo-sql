/**
 * jeefo-sqlite3-orm
 * https://github.com/je3f0o/jeefo-sqlite3-orm
 *
 * Copyright (c) 2014 je3f0o
 * Licensed under the MIT license.
 */

"use strict";

// Object.defineProperty(Array.prototype, "unique" {
// 	enumerable: false,
// 	configurable: false,
// 	writable: false,
// 	value: function () {
// 		var a = this.concat();
// 		for (var i = 0; i < a.length; ++i) {
// 			for (var j = i + 1; j < a.length; ++j) {
// 				if (a[i] === a[j])
// 					a.splice(j--, 1);
// 			}
// 		}

// 		return a;
// 	}
// });

var WhereNode = require("./where_node"),
	SqlQueryBuilder, p;

// Private methods
//
function build_wheres (query, $this, to_set_total) {
	if ($this.where_node) {
		var where_query = $this.where_node.build($this.order_fields);

		query.str   += " WHERE " + where_query.str;
		query.values = query.values.concat(where_query.values);

		if (to_set_total) {
			query.total += " WHERE " + where_query.str;
		}
	}
}

function is_int (n) {
	return (Number(n) === n) && (n % 1 === 0);
}

module.exports = SqlQueryBuilder = function (table) {
	this.table = table;
	return this.reset();
};
p = SqlQueryBuilder.prototype;

p.reset = function () {
	this.has_created_time = true;
	this.has_updated_time = true;
	this.fields           = {};
	this.where_node       = null;
	this.orders           = [];
	this.order_fields     = null;
	this._limit = {
		offset : 0,
		max    : 0
	};

	return this;
};

p.build_timestamps = function () {
	if (this.has_created_time || this.has_updated_time) {
		var date = new Date();

		if (this.has_created_time && !this.fields.created_at) {
			this.fields.created_at = date;
		}
		if (this.has_updated_time && !this.fields.updated_at) {
			this.fields.updated_at = date;
		}
	}

	return this;
};

p.limit = function (limit) {
	if (typeof(limit) === "object") {
		this._limit.offset = is_int(limit.offset) ? limit.offset : 0;
		this._limit.max    = is_int(limit.max)    ? limit.max    : 0;
	} else {
		this._limit.max = is_int(limit) ? limit : 0;
	}

	return this;
};

p.order_by = function (orders) {
	Object.keys(orders).forEach(function (key) {
		this.orders.push(key + " " + orders[key].toUpperCase());
	}, this);
};

p.bind = function (field, value) {
	if (typeof field === "object") {
		var fields = this.fields;
		Object.keys(field).forEach(function (field_name) {
			fields[field_name] = field[field_name];
		});
	} else {
		this.fields[field] = value;
	}
	return this;
};

p.where = function (wheres) {
	if (wheres.$limit) {
		this.limit(wheres.$limit);
		delete wheres.$limit;
	}

	if (wheres.$order_by) {
		this.order_by(wheres.$order_by);
		delete wheres.$order_by;
	}

	if (wheres.$to_sort) {
		this.order_fields = [];
		delete wheres.$to_sort;
	}

	if (Object.keys(wheres).length > 0) {
		this.where_node = new WhereNode(wheres);
	}

	return this;
};

/**
 * Builders ---------------------
 */
p.build_select_query = function () {
	// SQL_CALC_FOUND_ROWS
	var query = {
		str    : "SELECT * FROM `" + this.table + "`",
		total  : "SELECT COUNT(*) AS `total` FROM `" + this.table + "`",
		values : []
	};
	//SELECT FOUND_ROWS()

	build_wheres(query, this, true);
	query.total_values = query.values;

	if (this.order_fields) {
		this.order_fields.forEach(function (order_field) {
			query.str += " ORDER BY FIELD (" + order_field.key + ", " + order_field.place_holder + ")";
			query.values = query.values.concat(order_field.values);
		});
	}
	if (this.orders.length) {
		query.str += " ORDER BY " + this.orders.join(", ");
	}
	if (this._limit.max > 0) {
		query.str += " LIMIT " + this._limit.offset + ", " + this._limit.max;
	}

	return query;
};

p.build_insert_query = function () {

	this.build_timestamps();

	var query = {
			str    : "INSERT INTO `" + this.table + "`",
			values : []
		},
		place_holders = [],
		keys = Object.keys(this.fields);

	keys.forEach(function (key) {
		place_holders.push("?");
		query.values.push(this.fields[key]);
	}, this);

	query.str += "(`" + keys.join("`, `") + "`) ";
	query.str += "VALUES(" + place_holders.join(", ") + ")";

	return query;
};

p.build_update_query = function () {

	var query = {
			str    : "UPDATE `" + this.table + "` SET ",
			values : []
		},
		place_holders = [];

	this.has_created_time = false;
	this.build_timestamps();

	Object.keys(this.fields).forEach(function (key) {
		place_holders.push("`" + key + "` = ?");
		query.values.push(this.fields[key]);
	}, this);

	query.str += place_holders.join(", ");

	build_wheres(query, this);

	return query;
};

p.build_delete_query = function () {

	var query = {
		str    : "DELETE FROM `" + this.table + "`",
		values : []
	};

	build_wheres(query, this);

	return query;
};

p.build_replace_query = function () {

	var query = {
			str    : "REPLACE INTO `" + this.table + "`",
			values : []
		},
		place_holders = [],
		keys = Object.keys(this.fields);

	keys.forEach(function (key) {
		place_holders.push("?");
		query.values.push(this.fields[key]);
	}, this);

	query.str += "(`" + keys.join("`, `") + "`) ";
	query.str += "VALUES(" + place_holders.join(", ") + ")";

	return query;
};

if (require.main === module) {
	var sql = new module.exports("users");
	sql.where({
		id : 1
	});

	console.log(sql.build_select_query());
}
