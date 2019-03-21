// Copyright IBM Corp. 2012,2019. All Rights Reserved.
// Node module: loopback-connector-mysql
// This file is licensed under the MIT License.
// License text available at https://opensource.org/licenses/MIT

'use strict';
const g = require('strong-globalize')();

/*!
 * Module dependencies
 */
const mysql = require('mysql');

const SqlConnector = require('loopback-connector').SqlConnector;
const ParameterizedSQL = SqlConnector.ParameterizedSQL;
const EnumFactory = require('./enumFactory').EnumFactory;

const debug = require('debug')('loopback:connector:mysql');
const setHttpCode = require('./set-http-code');
const async = require('async');
const _ = require('lodash');

/**
 * @module loopback-connector-mysql
 *
 * Initialize the MySQL connector against the given data source
 *
 * @param {DataSource} dataSource The loopback-datasource-juggler dataSource
 * @param {Function} [callback] The callback function
 */
exports.initialize = function initializeDataSource(dataSource, callback) {
  dataSource.driver = mysql; // Provide access to the native driver
  dataSource.connector = new MySQL(dataSource.settings);
  dataSource.connector.dataSource = dataSource;

  defineMySQLTypes(dataSource);

  dataSource.EnumFactory = EnumFactory; // factory for Enums. Note that currently Enums can not be registered.

  if (callback) {
    if (dataSource.settings.lazyConnect) {
      process.nextTick(function() {
        callback();
      });
    } else {
      dataSource.connector.connect(callback);
    }
  }
};

exports.MySQL = MySQL;

function defineMySQLTypes(dataSource) {
  const modelBuilder = dataSource.modelBuilder;
  const defineType = modelBuilder.defineValueType ?
    // loopback-datasource-juggler 2.x
    modelBuilder.defineValueType.bind(modelBuilder) :
    // loopback-datasource-juggler 1.x
    modelBuilder.constructor.registerType.bind(modelBuilder.constructor);

  // The Point type is inherited from jugglingdb mysql adapter.
  // LoopBack uses GeoPoint instead.
  // The Point type can be removed at some point in the future.
  defineType(function Point() {
  });
}

/**
 * @constructor
 * Constructor for MySQL connector
 * @param {Object} client The node-mysql client object
 */
function MySQL(settings) {
  SqlConnector.call(this, 'mysql', settings);
}

require('util').inherits(MySQL, SqlConnector);

MySQL.prototype.connect = function(callback) {
  const self = this;
  const options = generateOptions(this.settings);
  const s = self.settings || {};

  if (this.client) {
    if (callback) {
      process.nextTick(function() {
        callback(null, self.client);
      });
    }
  } else {
    this.client = mysql.createPool(options);
    this.client.getConnection(function(err, connection) {
      const conn = connection;
      if (!err) {
        if (self.debug) {
          debug('MySQL connection is established: ' + self.settings || {});
        }
        connection.release();
      } else {
        if (self.debug || !callback) {
          console.error('MySQL connection is failed: ' + self.settings || {}, err);
        }
      }
      callback && callback(err, conn);
    });
  }
};

function generateOptions(settings) {
  const s = settings || {};
  if (s.collation) {
    // Charset should be first 'chunk' of collation.
    s.charset = s.collation.substr(0, s.collation.indexOf('_'));
  } else {
    s.collation = 'utf8_general_ci';
    s.charset = 'utf8';
  }

  s.supportBigNumbers = (s.supportBigNumbers || false);
  s.timezone = (s.timezone || 'local');

  if (isNaN(s.connectionLimit)) {
    s.connectionLimit = 10;
  }

  let options;
  if (s.url) {
    // use url to override other settings if url provided
    options = s.url;
  } else {
    options = {
      host: s.host || s.hostname || 'localhost',
      port: s.port || 3306,
      user: s.username || s.user,
      password: s.password,
      timezone: s.timezone,
      socketPath: s.socketPath,
      charset: s.collation.toUpperCase(), // Correct by docs despite seeming odd.
      supportBigNumbers: s.supportBigNumbers,
      connectionLimit: s.connectionLimit,
    };

    // Don't configure the DB if the pool can be used for multiple DBs
    if (!s.createDatabase) {
      options.database = s.database;
    }

    // Take other options for mysql driver
    // See https://github.com/loopbackio/loopback-connector-mysql/issues/46
    for (const p in s) {
      if (p === 'database' && s.createDatabase) {
        continue;
      }
      if (options[p] === undefined) {
        options[p] = s[p];
      }
    }
    // Legacy UTC Date Processing fallback - SHOULD BE TRANSITIONAL
    if (s.legacyUtcDateProcessing === undefined) {
      s.legacyUtcDateProcessing = true;
    }
    if (s.legacyUtcDateProcessing) {
      options.timezone = 'Z';
    }
  }
  return options;
}
/**
 * Execute the sql statement
 *
 * @param {String} sql The SQL statement
 * @param {Function} [callback] The callback after the SQL statement is executed
 */
MySQL.prototype.executeSQL = function(sql, params, options, callback) {
  const self = this;
  const client = this.client;
  const debugEnabled = debug.enabled;
  const db = this.settings.database;
  if (typeof callback !== 'function') {
    throw new Error(g.f('{{callback}} should be a function'));
  }
  if (debugEnabled) {
    debug('SQL: %s, params: %j', sql, params);
  }

  const transaction = options.transaction;

  function handleResponse(connection, err, result) {
    if (!transaction) {
      connection.release();
    }
    if (err) {
      err = setHttpCode(err);
    }
    callback && callback(err, result);
  }

  function runQuery(connection, release) {
    connection.query(sql, params, function(err, data) {
      if (debugEnabled) {
        if (err) {
          debug('Error: %j', err);
        }
        debug('Data: ', data);
      }
      handleResponse(connection, err, data);
    });
  }

  function executeWithConnection(err, connection) {
    if (err) {
      return callback && callback(err);
    }
    if (self.settings.createDatabase) {
      // Call USE db ...
      connection.query('USE ??', [db], function(err) {
        if (err) {
          if (err && err.message.match(/(^|: )unknown database/i)) {
            const charset = self.settings.charset;
            const collation = self.settings.collation;
            const q = 'CREATE DATABASE ?? CHARACTER SET ?? COLLATE ??';
            connection.query(q, [db, charset, collation], function(err) {
              if (!err) {
                connection.query('USE ??', [db], function(err) {
                  runQuery(connection);
                });
              } else {
                handleResponse(connection, err);
              }
            });
            return;
          } else {
            handleResponse(connection, err);
            return;
          }
        }
        runQuery(connection);
      });
    } else {
      // Bypass USE db
      runQuery(connection);
    }
  }

  if (transaction && transaction.connection &&
    transaction.connector === this) {
    if (debugEnabled) {
      debug('Execute SQL within a transaction');
    }
    executeWithConnection(null, transaction.connection);
  } else {
    client.getConnection(executeWithConnection);
  }
};

MySQL.prototype._modifyOrCreate = function(model, data, options, fields, cb) {
  const sql = new ParameterizedSQL('INSERT INTO ' + this.tableEscaped(model));
  const columnValues = fields.columnValues;
  const fieldNames = fields.names;
  if (fieldNames.length) {
    sql.merge('(' + fieldNames.join(',') + ')', '');
    const values = ParameterizedSQL.join(columnValues, ',');
    values.sql = 'VALUES(' + values.sql + ')';
    sql.merge(values);
  } else {
    sql.merge(this.buildInsertDefaultValues(model, data, options));
  }

  sql.merge('ON DUPLICATE KEY UPDATE');
  const setValues = [];
  for (let i = 0, n = fields.names.length; i < n; i++) {
    if (!fields.properties[i].id) {
      setValues.push(new ParameterizedSQL(fields.names[i] + '=' +
        columnValues[i].sql, columnValues[i].params));
    }
  }

  sql.merge(ParameterizedSQL.join(setValues, ','));

  this.execute(sql.sql, sql.params, options, function(err, info) {
    if (!err && info && info.insertId) {
      data.id = info.insertId;
    }
    const meta = {};
    // When using the INSERT ... ON DUPLICATE KEY UPDATE statement,
    // the returned value is as follows:
    // 1 for each successful INSERT.
    // 2 for each successful UPDATE.
    // 1 also for UPDATE with same values, so we cannot accurately
    // report if we have a new instance.
    meta.isNewInstance = undefined;
    cb(err, data, meta);
  });
};

/**
 * Replace if the model instance exists with the same id or create a new instance
 *
 * @param {String} model The model name
 * @param {Object} data The model instance data
 * @param {Object} options The options
 * @param {Function} [cb] The callback function
 */
MySQL.prototype.replaceOrCreate = function(model, data, options, cb) {
  const fields = this.buildReplaceFields(model, data);
  this._modifyOrCreate(model, data, options, fields, cb);
};

/**
 * Update if the model instance exists with the same id or create a new instance
 *
 * @param {String} model The model name
 * @param {Object} data The model instance data
 * @param {Object} options The options
 * @param {Function} [cb] The callback function
 */
MySQL.prototype.save =
  MySQL.prototype.updateOrCreate = function(model, data, options, cb) {
    const fields = this.buildFields(model, data);
    this._modifyOrCreate(model, data, options, fields, cb);
  };

MySQL.prototype.getInsertedId = function(model, info) {
  const insertedId = info && typeof info.insertId === 'number' ?
    info.insertId : undefined;
  return insertedId;
};

/*!
 * Convert property name/value to an escaped DB column value
 * @param {Object} prop Property descriptor
 * @param {*} val Property value
 * @returns {*} The escaped value of DB column
 */
MySQL.prototype.toColumnValue = function(prop, val) {
  if (val === undefined && this.isNullable(prop)) {
    return null;
  }
  if (val === null) {
    if (this.isNullable(prop)) {
      return val;
    } else {
      try {
        const castNull = prop.type(val);
        if (prop.type === Object) {
          return JSON.stringify(castNull);
        }
        return castNull;
      } catch (err) {
        // if we can't coerce null to a certain type,
        // we just return it
        return 'null';
      }
    }
  }
  if (!prop) {
    return val;
  }
  if (prop.type === String) {
    return String(val);
  }
  if (prop.type === Number) {
    if (isNaN(val)) {
      // FIXME: [rfeng] Should fail fast?
      return val;
    }
    return val;
  }
  if (prop.type === Date) {
    if (!val.toUTCString) {
      val = new Date(val);
    }
    return val;
  }
  if (prop.type.name === 'DateString') {
    return val.when;
  }
  if (prop.type === Boolean) {
    return !!val;
  }
  if (prop.type.name === 'GeoPoint') {
    return new ParameterizedSQL({
      sql: 'Point(?,?)',
      params: [val.lng, val.lat],
    });
  }
  if (prop.type === Buffer) {
    return val;
  }
  if (prop.type === Object) {
    return this._serializeObject(val);
  }
  if (typeof prop.type === 'function') {
    return this._serializeObject(val);
  }
  return this._serializeObject(val);
};

MySQL.prototype._serializeObject = function(obj) {
  let val;
  if (obj && typeof obj.toJSON === 'function') {
    obj = obj.toJSON();
  }
  if (typeof obj !== 'string') {
    val = JSON.stringify(obj);
  } else {
    val = obj;
  }
  return val;
};

/*!
 * Convert the data from database column to model property
 * @param {object} Model property descriptor
 * @param {*) val Column value
 * @returns {*} Model property value
 */
MySQL.prototype.fromColumnValue = function(prop, val) {
  if (val == null) {
    return val;
  }
  if (prop) {
    switch (prop.type.name) {
      case 'Number':
        val = Number(val);
        break;
      case 'String':
        val = String(val);
        break;
      case 'Date':
      case 'DateString':
        // MySQL allows, unless NO_ZERO_DATE is set, dummy date/time entries
        // new Date() will return Invalid Date for those, so we need to handle
        // those separate.
        if (!val || /^0{4}(-00){2}( (00:){2}0{2}(\.0{1,6}){0,1}){0,1}$/.test(val)) {
          val = null;
        }
        break;
      case 'Boolean':
        // BIT(1) case: <Buffer 01> for true and <Buffer 00> for false
        // CHAR(1) case: '1' for true and '0' for false
        // TINYINT(1) case: 1 for true and 0 for false
        val = Buffer.isBuffer(val) && val.length === 1 ? Boolean(val[0]) : Boolean(parseInt(val));
        break;
      case 'GeoPoint':
      case 'Point':
        val = {
          lng: val.x,
          lat: val.y,
        };
        break;
      case 'ObjectID':
        val = new prop.type(val);
        break;
      case 'Buffer':
        val = prop.type(val);
        break;
      case 'List':
      case 'Array':
      case 'Object':
      case 'JSON':
        if (typeof val === 'string') {
          val = JSON.parse(val);
        }
        break;
      default:
        break;
    }
  }
  return val;
};

/**
 * Escape an identifier such as the column name
 * @param {string} name A database identifier
 * @returns {string} The escaped database identifier
 */
MySQL.prototype.escapeName = function(name) {
  return this.client.escapeId(name);
};

/**
 * Build the LIMIT clause
 * @param {string} model Model name
 * @param {number} limit The limit
 * @param {number} offset The offset
 * @returns {string} The LIMIT clause
 */
MySQL.prototype._buildLimit = function(model, limit, offset) {
  if (isNaN(limit)) {
    limit = 0;
  }
  if (isNaN(offset)) {
    offset = 0;
  }
  if (!limit && !offset) {
    return '';
  }
  return 'LIMIT ' + (offset ? (offset + ',' + limit) : limit);
};

MySQL.prototype.applyPagination = function(model, stmt, filter) {
  const limitClause = this._buildLimit(model, filter.limit,
    filter.offset || filter.skip);
  return stmt.merge(limitClause);
};

/**
 * Get the place holder in SQL for identifiers, such as ??
 * @param {String} key Optional key, such as 1 or id
 * @returns {String} The place holder
 */
MySQL.prototype.getPlaceholderForIdentifier = function(key) {
  return '??';
};

/**
 * Get the place holder in SQL for values, such as :1 or ?
 * @param {String} key Optional key, such as 1 or id
 * @returns {String} The place holder
 */
MySQL.prototype.getPlaceholderForValue = function(key) {
  return '?';
};

MySQL.prototype.getCountForAffectedRows = function(model, info) {
  const affectedRows = info && typeof info.affectedRows === 'number' ?
    info.affectedRows : undefined;
  return affectedRows;
};

/**
 * Disconnect from MySQL
 */
MySQL.prototype.disconnect = function(cb) {
  if (this.debug) {
    debug('disconnect');
  }
  if (this.client) {
    this.client.end((err) => {
      this.client = null;
      cb(err);
    });
  } else {
    process.nextTick(cb);
  }
};

MySQL.prototype.ping = function(cb) {
  this.execute('SELECT 1 AS result', cb);
};

MySQL.prototype.buildExpression = function(columnName, operator, operatorValue,
  propertyDefinition) {
  let clause;
  switch (operator) {
    case 'regexp':
      clause = columnName + ' REGEXP ?';
      // By default, MySQL regexp is not case sensitive. (https://dev.mysql.com/doc/refman/5.7/en/regexp.html)
      // To allow case sensitive regexp query, it has to be binded to a `BINARY` type.
      // If ignore case is not specified, search it as case sensitive.
      if (!operatorValue.ignoreCase) {
        clause = columnName + ' REGEXP BINARY ?';
      }

      if (operatorValue.global)
        g.warn('{{MySQL}} {{regex}} syntax does not respect the {{`g`}} flag');

      if (operatorValue.multiline)
        g.warn('{{MySQL}} {{regex}} syntax does not respect the {{`m`}} flag');

      return new ParameterizedSQL(clause,
        [operatorValue.source]);
    case 'matchnl':
    case 'matchqe':
    case 'matchnlqe':
    case 'matchbool':
    case 'match':
      let mode;
      switch (operator) {
        case 'matchbool':
          mode = ' IN BOOLEAN MODE';
          break;
        case 'matchnl':
          mode = ' IN NATURAL LANGUAGE MODE';
          break;
        case 'matchqe':
          mode = ' WITH QUERY EXPANSION';
          break;
        case 'matchnlqe':
          mode = ' IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION';
          break;
        default:
          mode = '';
      }
      clause = ` MATCH (${columnName}) AGAINST (?${mode})`;

      return new ParameterizedSQL(clause, [operatorValue]);
  }

  // invoke the base implementation of `buildExpression`
  return this.invokeSuper('buildExpression', columnName, operator,
    operatorValue, propertyDefinition);
};

/**
 * Build a SQL SELECT statement
 * @param {String} model Model name
 * @param {Object} filter Filter object
 * @param {Object} options Options object
 * @returns {ParameterizedSQL} Statement object {sql: ..., params: [...]}
 */
MySQL.prototype.buildSelect = function(model, filter, options) {
  if (!filter.order) {
    var idNames = this.idNames(model);
    if (idNames && idNames.length) {
      filter.order = idNames;
    }
  }
  var tableAlias = getTableAlias(model);
  var selectStmt = new ParameterizedSQL('SELECT ' +
    this.buildColumnNames(model, filter, tableAlias) +
    ' FROM ' + this.tableEscaped(model) + ' ' + tableAlias + ' '
  );

  if (filter) {
    let aliases = {};
    if (filter.where) {
      var aliasCount = {};
      var joins = this.buildJoins(model, filter.where, tableAlias, aliasCount);
      if (joins.joinStmt.sql) {
        aliases = joins.aliases;
        selectStmt.sql = selectStmt.sql.replace('SELECT', 'SELECT DISTINCT ' +
          tableAlias + '.id, ');
        selectStmt.merge(joins.joinStmt);
      }
      var whereStmt = this.buildWhere(model, filter.where, tableAlias, aliases);
      selectStmt.merge(whereStmt);
    }

    if (filter.order) {
      var order = this.buildOrderBy(model, filter.order, aliases);
      selectStmt.merge(order.orderBy);
      if (order.columnNames) {
        selectStmt.sql = selectStmt.sql.replace('FROM', ', ' +
          order.columnNames + ' FROM ');
      }
    }

    if (filter.limit || filter.skip || filter.offset) {
      selectStmt = this.applyPagination(
        model, selectStmt, filter);
    }
  }
  return this.parameterize(selectStmt);
};

/**
 * Build the SQL WHERE clause for the where object
 * @param {string} model Model name
 * @param {object} where An object for the where conditions
 * @param {string} tableAlias Alias to subselects
 * @param {string} aliases Created aliases
 * @returns {ParameterizedSQL} The SQL WHERE clause
 */
MySQL.prototype.buildWhere = function(model, where, tableAlias, aliases) {
  var whereClause = this._buildWhere(model, where, tableAlias, aliases);
  if (whereClause.sql) {
    whereClause.sql = 'WHERE ' + whereClause.sql;
  }
  return whereClause;
};

/*!
 * @param model
 * @param where
 * @returns {ParameterizedSQL}
 * @private
 */
MySQL.prototype._buildWhere = function(model, where, tableAlias, aliases) {
  if (!where) {
    return new ParameterizedSQL('');
  }
  if (typeof where !== 'object' || Array.isArray(where)) {
    debug('Invalid value for where: %j', where);
    return new ParameterizedSQL('');
  }
  var self = this;
  var props = self.getModelDefinition(model).properties;

  var whereStmts = [];
  for (var key in where) {
    var stmt = new ParameterizedSQL('', []);
    // Handle and/or operators
    if (key === 'and' || key === 'or') {
      var branches = [];
      var branchParams = [];
      var clauses = where[key];
      if (Array.isArray(clauses)) {
        for (var i = 0, n = clauses.length; i < n; i++) {
          var stmtForClause = self._buildWhere(model, clauses[i], tableAlias, aliases);
          if (stmtForClause.sql) {
            stmtForClause.sql = '(' + stmtForClause.sql + ')';
            branchParams = branchParams.concat(stmtForClause.params);
            branches.push(stmtForClause.sql);
          }
        }
        stmt.merge({
          sql: ' ( ' + branches.join(' ' + key.toUpperCase() + ' ') + ' ) ',
          params: branchParams,
        });
        whereStmts.push(stmt);
        continue;
      }
      // The value is not an array, fall back to regular fields
    }
    var p = props[key];
    if (p == null) {
      let relations = self.getModelDefinition(model).settings.relations;
      if (relations && relations[key]) {
        let relation = relations[key];
        let childWhere = self._buildWhere(relation.model, where[key],
          aliases[relation.model], aliases);
        whereStmts.push(childWhere);
      } else if (key == 'raw') {
        stmt.merge(where[key]);
        whereStmts.push(stmt);
      } else {
        debug('Unknown property %s is skipped for model %s', key, model);
      }
      continue;
    }
    /* eslint-disable one-var */
    var columnEscaped = self.columnEscaped(model, key);
    var columnName = tableAlias ? tableAlias + '.' + columnEscaped : columnEscaped;
    var expression = where[key];
    var columnValue;
    var sqlExp;
    /* eslint-enable one-var */
    if (expression === null || expression === undefined) {
      stmt.merge(columnName + ' IS NULL');
    } else if (expression && expression.constructor === Object) {
      var operator = Object.keys(expression)[0];
      // Get the expression without the operator
      expression = expression[operator];
      if (operator === 'inq' || operator === 'nin' || operator === 'between') {
        columnValue = [];
        if (Array.isArray(expression)) {
          // Column value is a list
          for (var j = 0, m = expression.length; j < m; j++) {
            columnValue.push(this.toColumnValue(p, expression[j]));
          }
        } else {
          columnValue.push(this.toColumnValue(p, expression));
        }
        if (operator === 'between') {
          // BETWEEN v1 AND v2
          var v1 = columnValue[0] === undefined ? null : columnValue[0];
          var v2 = columnValue[1] === undefined ? null : columnValue[1];
          columnValue = [v1, v2];
        } else {
          // IN (v1,v2,v3) or NOT IN (v1,v2,v3)
          if (columnValue.length === 0) {
            if (operator === 'inq') {
              columnValue = [null];
            } else {
              // nin () is true
              continue;
            }
          }
        }
      } else if (operator === 'regexp' && expression instanceof RegExp) {
        // do not coerce RegExp based on property definitions
        columnValue = expression;
      } else {
        columnValue = this.toColumnValue(p, expression);
      }
      sqlExp = self.buildExpression(
        columnName, operator, columnValue, p);
      stmt.merge(sqlExp);
    } else {
      // The expression is the field value, not a condition
      columnValue = self.toColumnValue(p, expression);
      if (columnValue === null) {
        stmt.merge(columnName + ' IS NULL');
      } else {
        if (columnValue instanceof ParameterizedSQL) {
          stmt.merge(columnName + '=').merge(columnValue);
        } else {
          stmt.merge({
            sql: columnName + '=?',
            params: [columnValue],
          });
        }
      }
    }
    whereStmts.push(stmt);
  }
  var params = [];
  var sqls = [];
  for (var k = 0, s = whereStmts.length; k < s; k++) {
    sqls.push(whereStmts[k].sql);
    params = params.concat(whereStmts[k].params);
  }
  var whereStmt = new ParameterizedSQL({
    sql: sqls.join(' AND '),
    params: params,
  });
  return whereStmt;
};

MySQL.prototype.buildJoins = function(model, where, tableAlias, aliasCount) {
  var self = this;
  var props = self.getModelDefinition(model).properties;

  var aliases = {};
  var joinStmts = [];
  for (var key in where) {
    // Handle and/or operators
    if (key === 'and' || key === 'or') {
      var stmt = new ParameterizedSQL('', []);
      var branches = [];
      var branchParams = [];
      var clauses = where[key];
      if (Array.isArray(clauses)) {
        for (var i = 0, n = clauses.length; i < n; i++) {
          var stmtForClause = self.buildJoins(model, clauses[i], tableAlias, aliasCount);
          aliases = Object.assign(aliases, stmtForClause.aliases);
          if (stmtForClause.joinStmt.sql) {
            branchParams = branchParams.concat(stmtForClause.joinStmt.params);
            branches.push(stmtForClause.joinStmt.sql);
          }
        }
        stmt.merge({
          sql: branches.join(' '),
          params: branchParams,
        });
        joinStmts.push(stmt);
        continue;
      }
      // The value is not an array, fall back to regular fields
    }
    var p = props[key];
    if (p == null) {
      let relations = self.getModelDefinition(model).settings.relations;
      if (relations && relations[key]) {
        let relation = relations[key];
        let childTableAlias = getTableAlias(relation.model, aliasCount);
        aliases[relation.model] = childTableAlias;
        let foreignKey = (relation.foreignKey && relation.foreignKey != '') ? relation.foreignKey :
          relation.model.toLowerCase() + 'Id';
        let type = relation.type;
        let joinThrough, joinOn;
        if (type == 'belongsTo') {
          joinOn = tableAlias + '.' + foreignKey + ' = ' + childTableAlias + '.id';
        } else if (type == 'hasMany' && relation.through) {
          const throughModel = this.tableEscaped(relation.through);
          const throughModelAlias = getTableAlias(relation.through, aliasCount);
          aliases[relation.through] = throughModelAlias;
          let parentForeignKey = relation.foreignKey ||
            lowerCaseFirstLetter(model) + 'Id';
          let childForeignKey = relation.keyThrough ||
            lowerCaseFirstLetter(relation.model) + 'Id';
          joinThrough = `LEFT JOIN ${throughModel} ${throughModelAlias} 
            ON ${throughModelAlias}.${parentForeignKey}` +
            ` = ${tableAlias}.id`;
          joinOn = throughModelAlias + '.' + childForeignKey +
            ' = ' + childTableAlias + '.id';
        } else {
          joinOn = childTableAlias + '.' + foreignKey + ' = ' + tableAlias + '.id';
        }
        const joinTable = this.tableEscaped(relation.model);
        let join = new ParameterizedSQL(
          `LEFT JOIN ${joinTable} ${childTableAlias} ON ${joinOn}`, []);
        if (joinThrough) {
          join = new ParameterizedSQL(joinThrough, []).merge(join);
        }
        var recursiveResult = self.buildJoins(relation.model, where[key],
          childTableAlias, aliasCount);
        join.merge(recursiveResult.joinStmt);
        aliases = Object.assign(aliases, recursiveResult.aliases);
        joinStmts.push(join);
      } else {
        // Unknown property, ignore it
        debug('Unknown property %s is skipped for model %s', key, model);
      }
    }
  }
  var params = [];
  var sqls = [];
  for (var k = 0, s = joinStmts.length; k < s; k++) {
    sqls.push(joinStmts[k].sql);
    params = params.concat(joinStmts[k].params);
  }
  var joinStmt = new ParameterizedSQL({
    sql: sqls.join(' '),
    params: params,
  });
  var result = {
    aliases: aliases,
    joinStmt: joinStmt,
  };
  return result;
};

/**
 * Build the ORDER BY clause
 * @param {string} model Model name
 * @param {string[]} order An array of sorting criteria
 * @returns {string} The ORDER BY clause
 */
MySQL.prototype.buildOrderBy = function(model, order, aliases) {
  if (!order) {
    return '';
  }
  var self = this;
  if (typeof order === 'string') {
    order = [order];
  }
  var clauses = [];
  var columnNames = [];
  for (var i = 0, n = order.length; i < n; i++) {
    var sign = '';
    var t = order[i].split(/[\s,]+/);
    if (t[0].indexOf('-') === 0) {
      sign = '-';
      t[0] = t[0].substr(1);
    }
    if (t.length === 1) {
      clauses.push(sign + self.columnEscaped(model, order[i]));
    } else {
      var key = t[0];
      if (key.indexOf('.') > -1) {
        const modelAndProperty = key.split('.');
        const relations = this.getModelDefinition(model).settings.relations;
        if (relations && relations[modelAndProperty[0]]) {
          let relation = relations[modelAndProperty[0]];
          const alias = aliases[relation.model];
          if (alias) {
            clauses.push(sign + alias + '.' + modelAndProperty[1] + ' ' + t[1]);
            columnNames.push(alias + '.' + modelAndProperty[1] +
              ' as ' + alias + '_orderBy' + modelAndProperty[1]);
          }
        }
      } else {
        clauses.push(sign + self.columnEscaped(model, t[0]) + ' ' + t[1]);
      }
    }
  }
  var result = {
    orderBy: clauses.length > 0 ? 'ORDER BY ' + clauses.join(',') : '',
    columnNames: columnNames.join(','),
  };
  return result;
};

/**
 * Build a list of escaped column names for the given model and fields filter
 * @param {string} model Model name
 * @param {object} filter The filter object
 * @param {object} tableAlias The table alias
 * @returns {string} Comma separated string of escaped column names
 */
MySQL.prototype.buildColumnNames = function(model, filter, tableAlias) {
  var fieldsFilter = filter && filter.fields;
  var cols = this.getModelDefinition(model).properties;
  if (!cols) {
    return tableAlias ? tableAlias + '.*' : '*';
  }
  var self = this;
  var keys = Object.keys(cols);
  if (Array.isArray(fieldsFilter) && fieldsFilter.length > 0) {
    // Not empty array, including all the fields that are valid properties
    keys = fieldsFilter.filter(function(f) {
      return cols[f];
    });
  } else if ('object' === typeof fieldsFilter &&
    Object.keys(fieldsFilter).length > 0) {
    // { field1: boolean, field2: boolean ... }
    var included = [];
    var excluded = [];
    keys.forEach(function(k) {
      if (fieldsFilter[k]) {
        included.push(k);
      } else if ((k in fieldsFilter) && !fieldsFilter[k]) {
        excluded.push(k);
      }
    });
    if (included.length > 0) {
      keys = included;
    } else if (excluded.length > 0) {
      excluded.forEach(function(e) {
        var index = keys.indexOf(e);
        keys.splice(index, 1);
      });
    }
  }
  var names = keys.map(function(c) {
    const columnEscaped = self.columnEscaped(model, c);
    return tableAlias ? tableAlias + '.' + columnEscaped : columnEscaped;
  });
  return names.join(',');
};

/**
 * Count all model instances by the where filter
 *
 * @param {String} model The model name
 * @param {Object} where The where object
 * @param {Object} options The options object
 * @param {Function} cb The callback function
 */
MySQL.prototype.count = function(model, where, options, cb) {
  if (typeof where === 'function') {
    // Backward compatibility for 1.x style signature:
    // count(model, cb, where)
    var tmp = options;
    cb = where;
    where = tmp;
  }

  var tableAlias = getTableAlias(model);

  var stmt = new ParameterizedSQL('SELECT count(*) as "cnt" FROM ' +
    this.tableEscaped(model) + ' ' + tableAlias + ' ');
  var aliasCount = {};
  var joins = this.buildJoins(model, where, tableAlias, aliasCount);
  var aliases = {};
  if (joins.joinStmt.sql) {
    stmt.sql = stmt.sql.replace('count(*)', 'COUNT(DISTINCT ' + tableAlias + '.id)');
    stmt.merge(joins.joinStmt);
    aliases = joins.aliases;
  }
  stmt = stmt.merge(this.buildWhere(model, where, tableAlias, aliases));
  stmt = this.parameterize(stmt);
  this.execute(stmt.sql, stmt.params,
    function(err, res) {
      if (err) {
        return cb(err);
      }
      var c = (res && res[0] && res[0].cnt) || 0;
      // Some drivers return count as a string to contain bigint
      // See https://github.com/brianc/node-postgres/pull/427
      cb(err, Number(c));
    });
};

/**
 * Find matching model instances by the filter
 *
 * Please also note the name `all` is confusing. `Model.find` is to find all
 * matching instances while `Model.findById` is to find an instance by id. On
 * the other hand, `Connector.prototype.all` implements `Model.find` while
 * `Connector.prototype.find` implements `Model.findById` due to the `bad`
 * naming convention we inherited from juggling-db.
 *
 * @param {String} model The model name
 * @param {Object} filter The filter
 * @param {Function} [cb] The cb function
 */
MySQL.prototype.all = function find(model, filter, options, cb) {
  var self = this;
  // Order by id if no order is specified
  filter = filter || {};
  var stmt = this.buildSelect(model, filter, options);
  this.execute(stmt.sql, stmt.params, options, function(err, data) {
    if (err) {
      return cb(err, []);
    }

    var objs = data.map(function(obj) {
      return self.fromRow(model, obj);
    });
    if (filter && filter.include) {

      var include = filter.include;
      var subqueries = [];
      if (Array.isArray(include)) {
        subqueries = include.filter(includeItem => includeItem && includeItem.type == 'subquery');
        include = include.filter(includeItem => !(includeItem && includeItem.type == 'subquery'));
      }
      
      self.getModelDefinition(model).model.include(
        objs, include, options, function (err) {
          if (err) {
            return cb(err, []);
          }
          self.doCustomSubqueriesAndGetIncludeWithoutThem(objs, options.enableSql ? subqueries : [], function (err) {
            if (err) {
              return cb(err, []);
            }
            cb(null, objs);      
          });          
        });
    } else {
      cb(null, objs);
    }
  });
};

/**
 * Find matching model instances by the filter
 * 
 */

MySQL.prototype.doCustomSubqueriesAndGetIncludeWithoutThem = function doCustomSubqueriesAndGetIncludeWithoutThem(objs, subqueries, cb) {
  var self = this;

  async.eachSeries(subqueries,
    function (subquery, nextSubquery) {
      if (!(subquery && subquery.sql && subquery.property)) {
        debug('subquery include type need "sql" and "property" options');
        return nextSubquery();
      }
      async.eachLimit(objs, subquery.eachLimit || 10,
        function (obj, nextObj) {
          var params = [];
          try {
            var subqueryParams = subquery.params;
            if (subqueryParams) {
              params = subqueryParams.map(function (subqueryParam) {
                var param = subqueryParam;
                if (subqueryParam && subqueryParam.indexOf && subqueryParam.indexOf('__') > -1) {
                  param = _.get(JSON.parse(JSON.stringify(obj)) , subqueryParam.replace('__', ''), null);
                }
                return param;
              });
            }
          } catch (e) {
            return nextObj(e, null);
          } 
          self.execute(subquery.sql, params, {}, function (err, data) {
            if (err) {
              return nextObj(err);
            }
            var result = data;
            if (subquery.result == 'hasOne' && data && data.length > 0) {
              result = data[0];
            }
            obj[subquery.property] = result;
            nextObj();
          });
        }, nextSubquery);
    },
    function (err) {
      cb(err);
    });
};

function getTableAlias(model, aliasCount) {
  let modelAlias = model;
  if (aliasCount) {
    if (!aliasCount[model]) {
      aliasCount[model] = 0;
    }
    aliasCount[model]++;
    modelAlias += aliasCount[model];
  }
  return modelAlias;
}

function lowerCaseFirstLetter(text) {
  return text.charAt(0).toLowerCase() + text.slice(1);
}

require('./migration')(MySQL, mysql);
require('./discovery')(MySQL, mysql);
require('./transaction')(MySQL, mysql);
