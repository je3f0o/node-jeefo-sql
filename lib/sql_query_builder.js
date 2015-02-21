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
	if ($this.wheres.length) {
		var where_str = " WHERE ";

		$this.wheres.forEach(function (where, index) {
			var where_query = where.build($this.order_fields);

			if (index > 0) {
				where_str += " OR ";
			}

			where_str   += where_query.str;
			query.values = query.values.concat(where_query.values);

			if (where_query.sort) {
				$this.order_fields.push(where_query.sort);
			}
		});

		query.str += where_str;
		if (to_set_total) {
			query.total += where_str;
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
	this.wheres           = [];
	this.orders           = [];
	this.order_fields     = [];
	this.has_ordered      = false;
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

	if (wheres.$groups) {
		wheres.$groups.forEach(function (group) {
			this.wheres.push(new WhereNode(group));
		}, this);

		delete wheres.$groups;
	}

	if (Object.keys(wheres).length > 0) {
		this.wheres.unshift(new WhereNode(wheres));
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

	this.order_fields.forEach(function (order_field, index) {
		if (index === 0) {
			query.str += " ORDER BY ";
			this.has_ordered = true;
		} else {
			query.str += ", ";
		}

		query.str   += "FIELD (" + order_field.key + ", " + order_field.place_holder + ")";
		query.values = query.values.concat(order_field.values);
	}, this);

	if (this.orders.length) {
		if (! this.has_ordered) {
			query.str += " ORDER BY ";
		}

		query.str += this.orders.join(", ");
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
		]
	});

	var query = sql.build_select_query();
	console.log(query.str);
	console.log(query.values);
	console.log("-------------");
	console.log(query.total);
	console.log(query.total_values);
}
