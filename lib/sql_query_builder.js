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

var _operators = {
	$lt   : "<",
	$lte  : "<=",
	$gt   : ">",
	$gte  : ">=",
	$like : "LIKE"
};

// private methods
// 
function build_timestamps(qb) {
	if (qb.has_created_time || qb.has_updated_time) {
		var date = new Date();

		if (qb.has_created_time && !qb.fields.created_at) {
			qb.fields.created_at = date;
		}
		if (qb.has_updated_time && !qb.fields.updated_at) {
			qb.fields.updated_at = date;
		}
	}
}

function build_wheres($this, query, is_select_query) {

	var wheres = $this.wheres,
		place_holders = [];

	Object.keys(wheres.not).forEach(function (key) {
		wheres.not[key].forEach(function (val) {
			place_holders.push("`" + key + "` <> ?");
			query.values.push(val);
		});
	});

	Object.keys(wheres.is).forEach(function (key) {
		place_holders.push("`" + key + "` = ?");
		query.values.push(wheres.is[key]);
	});

	Object.keys(wheres.like).forEach(function (key) {
		wheres.like[key].forEach(function (val) {
			place_holders.push("`" + key + "` LIKE ?");
			query.values.push(val);
		});
	});

	Object.keys(wheres.in).forEach(function (key) {
		var temp_place_holders = Array.apply(null, new Array(wheres.in[key].length))
			.map(String.prototype.valueOf, "?")
			.join(", ");

		place_holders.push("`" + key + "` IN (" + temp_place_holders + ")");

		wheres.in[key].forEach(function (value) {
			query.values.push(value);
		});
		wheres.in[key].forEach(function (value) {
			query.values.push(value);
		});

		$this.order_field = {
			key    : key,
			values : temp_place_holders
		};
	});

	Object.keys(wheres.operators).forEach(function (key) {
		var operator = wheres.operators[key],
			op = _operators[key];
		
		if (op === void 0) { throw new Error("Invalid operator : " + key); }

		Object.keys(operator).forEach(function (field) {
			place_holders.push("`" + field + "` " + op + " ?");
			query.values.push(operator[field]);
		});
	});

	if (place_holders.length > 0) {
		query.str += " WHERE " + place_holders.join(" AND ");
		if (is_select_query) {
			query.total += " WHERE " + place_holders.join(" AND ");
		}
	}
}

function SqlQueryBuilder(table) {
	this.table = table;
	return this.reset();
}

var p = SqlQueryBuilder.prototype;

p.reset = function () {
	this.has_created_time = true;
	this.has_updated_time = true;
	this.fields = {};
	this.wheres = {
		is: {},
		not: {},
		like: {},
		in : {},
		operators: {}
	};
	this.orders = [];
	this.order_field = null;
	this._limit = {
		offset : 0,
		max    : 0
	};

	return this;
};

p.limit = function (offset, max) {
	if (max === void 0) {
		max = offset;
		offset = 0;
	}

	this._limit.offset = offset;
	this._limit.max    = max;

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

p.where_is = function (field_name, values) {
	var where_is = this.wheres.is;

	if (!where_is[field_name]) {
		where_is[field_name] = [];
	}

	var field = where_is[field_name];

	if (values instanceof Array) {
		values.forEach(function (v) {
			field.push(v);
		});
	} else {
		field.push(values);
	}

	return this;
};

p.where_in = function (field, values) {
	if (typeof field === "object") {
		Object.keys(field).forEach(function (name) {
			this.wheres.in[name] = field[name];
		}, this);
	} else {
		this.wheres.in[field] = values;
	}
	return this;
};

p.where_operators = function (field, values) {
	Object.keys(values).forEach(function (op) {
		var where_operators = this.wheres.operators;
		if (!where_operators[op]) {
			where_operators[op] = {};
		}
		where_operators[op][field] = values[op];
	}, this);
};

p.where = function (wheres) {
	Object.keys(wheres).forEach(function (name) {
		var values = wheres[name];

		if (name === "$limit") {
			if (typeof values === "number") {
				this.limit(values);
			} else {
				this.limit(values.offset, values.max);
			}
		} else if (name === "$order_by") {
			this.order_by(values);
		} else if (typeof values === "object") {
			if (values instanceof Array) {
				this.where_in(name, values);
			} else if (values instanceof Date){
				this.wheres.is[name] = values;
			} else {
				this.where_operators(name, values);
			}
		} else {
			this.wheres.is[name] = values;
		}

	}, this);
	// console.log(this);
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

	build_wheres(this, query, true);

	if (this.order_field) {
		query.str += " ORDER BY FIELD (" + this.order_field.key + ", " + this.order_field.values + ")";
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

	build_timestamps(this);

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
	build_timestamps(this);

	Object.keys(this.fields).forEach(function (key) {
		place_holders.push("`" + key + "` = ?");
		query.values.push(this.fields[key]);
	}, this);

	query.str += place_holders.join(", ");

	build_wheres(this, query);

	return query;
};

p.build_delete_query = function () {

	var query = {
		str    : "DELETE FROM `" + this.table + "`",
		values : []
	};

	build_wheres(this, query);

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

module.exports = SqlQueryBuilder;
