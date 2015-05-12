
var async = require('async');
var mysql = require('mysql');
var _ = require('underscore');
var noop = function(){};

(function(Merger) {
	var atkns = 'atkns';

	//var vb = 'vb';
	//var teams = 'teams';
	//var main = 'main';


	// the 3 DBs names
	var vb = 'vb_atkins';
	var teams = 'atkins_teams';
	var main = 'atkins_users';

	var vbUsersMapToMain = {};

	var q = Merger.query = function(query, callback) {
		if (!Merger.connection) {
			var err = {error: 'MySQL connection is not setup. Run setup(config) first'};
			return callback(err);
		}
		console.log('\n\n====QUERY====\n\n' + query + '\n');
		Merger.connection.query(query, function(err, rows) {
			if (rows && typeof rows.length === 'number') {
				console.log('returned: ' + rows.length + ' results');
			}
			callback(err, rows)
		});
	};

	var copyTableStruct = Merger.copyTableStruct = function (tableName, fromDbName, toDbName, callback) {
		async.series([
					function(next) {
						q('DROP TABLE IF EXISTS ' + toDbName + '.' + tableName, next);
					},
					function(next) {
						q('CREATE TABLE ' + toDbName + '.' + tableName + ' LIKE ' + fromDbName + '.' + tableName, next);
					}
				],
				function (err, results) {
					callback(err, results);
				})
	};


	var copyTable = Merger.copyTable = function (tableName, fromDbName, toDbName, callback) {
		async.series([
					function(next) {
						copyTableStruct(tableName, fromDbName, toDbName, next);
					},
					function(next) {
						q('INSERT ' + toDbName + '.' + tableName + ' SELECT * FROM ' + fromDbName + '.' + tableName, next);
					}
				],
				function (err, results) {
					callback(err, results);
				})
	};

	var addColumn = Merger.addColumn = function (columnName, columnType, tableName, dbName, callback) {
		async.series([
			function(next) {
				q('ALTER TABLE ' + dbName + '.' + tableName + ' DROP COLUMN ' + columnName, function(err) {
					err && console.log("IGNORED ERROR", err);
					// ignore error if the column does not exists
					next();
				});
			},
			function(next) {
				q('ALTER TABLE ' + dbName + '.' + tableName + ' ADD ' + columnName + ' ' + columnType , next);
			}
		], function(err) {
			callback(err);
		});
	};

	Merger.run = function (config) {
		config = config || {};

		var _config = {
			host: config.host || 'localhost',
			user: config.user || 'user',
			password: config.password || 'password',
			port: config.port || 3306
		};

		Merger.connection = mysql.createConnection(_config);
		Merger.connection.connect();

		async.series([
			function(next) {
				q('DROP DATABASE IF EXISTS ' + atkns, next);
			},

			function(next) {
				q('CREATE DATABASE ' + atkns, next);
			},

			function(next) {
				copyTableStruct('user', vb, atkns, next);
			},

			function(next) {
				copyTableStruct('customavatar', vb, atkns, next);
			},

			function(next) {
				copyTableStruct('usergroup', vb, atkns, next);
			},

			function(next) {
				copyTableStruct('usergroupleader', vb, atkns, next);
			},

			function(next) {
				copyTableStruct('pm', vb, atkns, next);
			},

			function(next) {
				copyTableStruct('pmtext', vb, atkns, next);
			},

			function(next) {
				copyTableStruct('thread', vb, atkns, next);
			},

			function(next) {
				copyTableStruct('post', vb, atkns, next);
			},

			function(next) {
				copyTable('forum', vb, atkns, next);
			},

			function(next) {
				addColumn('timestamp', 'VARCHAR(50)', 'usergroup', atkns, next);
			},

			function(next) {
				var usersSelect = 'SELECT '
						+ main + '.Users.UserName as username, '
						+ main + '.Users.UserID as userid, '
						+ main + '.Users.Email as email, '
						+ vb + '.user.joindate as joindate, '
						+ vb + '.user.password as password, '
						+ vb + '.user.homepage as homepage, '
						+ vb + '.user.reputation as reputation, '
						+ vb + '.user.profilevisits as profileviews, '
						+ vb + '.user.birthday as birthday, '

						+ vb + '.sigparsed.signatureparsed as signatureparsed, '
						+ vb + '.customavatar.filename as customavatar__filename, '
						+ vb + '.customavatar.filedata as customavatar__filedata, '

						+ vb + '.user.username as _v_username, '
						+ vb + '.user.userid as _v_uid, '
						+ vb + '.user.email as _v_email, '
						+ teams + '.users.UserName as _t_username, '
						+ teams + '.users.UserID as _t_uid '

						+ 'FROM ' + main + '.Users '
						+ 'LEFT JOIN ' + teams + '.users ON ' + teams + '.users.UserID = ' + main + '.Users.UserID '
						+ 'LEFT JOIN ' + vb + '.user ON ' + vb + '.user.username = ' + main + '.users.UserName '
						+ 'LEFT JOIN ' + vb + '.sigparsed ON ' + vb + '.sigparsed.userid = ' + vb + '.user.userid '
						+ 'LEFT JOIN ' + vb + '.customavatar ON ' + vb + '.customavatar.userid = ' + vb + '.user.userid '
						+ 'WHERE ' + vb + '.user.posts > 0 OR ( ' + teams + '.users.UserID IS NOT NULL AND ' + teams + '.users.PostCount > 0 ) '

						+ 'GROUP BY userid '

						// + 'LIMIT 1000'

						+ '';

				// separating this into a second query because it makes the 1st one too, like too slow.
				var membergroupidsSelect = 'SELECT '
						+ teams + '.teamusers.UserID as userid, '
						+ 'GROUP_CONCAT(' + teams + '.teamusers.TeamID SEPARATOR \',\') as membergroupids '
						+ 'FROM ' + teams + '.teamusers '
						+ 'GROUP BY userid '
						+ '';

				var onUserSelect = function (err, users) {
					if (err) {
						throw err;
					}

					var userInsert = 'INSERT INTO ' + atkns + '.user (userid, username, email, membergroupids, joindate, password, homepage, reputation, profileviews, birthday) ';
					var sigparsedInsert = 'INSERT INTO ' + atkns + '.sigparsed (userid, signatureparsed) ';
					var customavatarInsert = 'INSERT INTO ' + atkns + '.customavatar (userid, filename, filedata) ';

					q(membergroupidsSelect, function(err, groupsIds) {
						var userGroups = _.indexBy(groupsIds, 'userid');

						var uc = 0;
						var sc = 0;
						var ac = 0;

						users.forEach(function(user, i) {

							user.membergroupids = userGroups[user.userid] || '';
							user.homepage = user.homepage || '';
							user.reputation = user.reputation || 0;
							user.profileviews = user.profileviews || 0;
							user.birthday = user.birthday || '';

							vbUsersMapToMain[user._v_uid] = user;

							if (user.userid && user.username && user.email) {
								if (uc++) {
									userInsert += ',';
								}
								userInsert += ' (' + user.userid + ',"' + user.username + '","' + user.email + '","' + user.membergroupids + '",' + user.joindate + ',"' + user.password + '","' + user.homepage + '",' + user.reputation + ',' + user.profileviews + ',"' + user.birthday + '") ';
							}

							if (user.signatureparsed) {
								if (sc++) {
									sigparsedInsert += ','
								}
								sigparsedInsert += ' (' + user.userid + ',"' + user.signatureparsed + '") ';
							}

							if (user.filename) {
								if (ac++) {
									customavatarInsert += ','
								}
								customavatarInsert += ' (' + user.userid + ',"' + user.filename + '","' + user.filedata + '") ';
							}
						});
						console.log("userInsert values: ", uc);

						console.log("sigparsedInsert values: ", sc);

						console.log("customavatarInsert values: ", ac);

						async.series([
							function(next) {
								q(userInsert, next);
							},
							function() {
								if (sc) {
									q(sigparsedInsert, next);
								} else {
									next();
								}
							},
							function() {
								if (ac) {
									q(customavatarInsert, next);
								} else {
									next();
								}
							},
							function() {}
						], function(err) {
							if (err) throw err;

							// next();
						});
					});
				}

				q(usersSelect, onUserSelect);
			},

			function(next) {
				next();
			},

			function(next) {
				next();
			},
			function(next) {
				next();
			}
		], function(err) {
			err && console.log(err);

			console.log('done');
		});
	};

	// just 'node tmp.js' to run it
	Merger.run();

})(module.exports);
