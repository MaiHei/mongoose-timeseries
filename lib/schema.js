'use strict';
/**
 * Module dependencies.
 */
var _ = require("underscore"),
    mongoose = require('mongoose'),
    moment = require('moment'),
    Schema = mongoose.Schema,
    Mixed = mongoose.Schema.Types.Mixed;

var TimeSeriesSchema = function (args) {
    /*
     allowed interval values (seconds):
     0.001, 0.002, 0.005, 0.01, 0,02, 0,05, 0.1, 0.2, 0.5,       //milliseconds
     1, 2, 5, 10, 30,  60,                                       //seconds
     120 (2min), 300 (5min), 600 (10min), 1800(30min), 3600(1h)  //minutes
     */

    var options = {
            interval: 60,  // seconds
            millisecond: false,
            verbose: false,
            postProcessImmediately: true,
            paths: {value: {type: 'number'}, metadata: {type: Mixed}}
        },
        roundDay = function (d) {
            return moment({year: d.get('year'), month: d.get('month'), day: d.get('date')}).startOf('day');
        },
        schema = new Schema({
            day: {type: Date, index: true, required: true},
            metadata: {
                interval: {type: Number}
            },
            latest: {
                timestamp: {type: Date},
                value: {type: Number, default: 0},
                metadata: {type: Mixed}
            },
            createdAt: {
                date: {type: Date, default: Date},
                user: {type: String}
            },
            updatedAt: {
                date: {type: Date},
                user: {type: String}
            },
            statistics: {
                i: {type: Number, default: 0},
                avg: {type: Number},
                max: {
                    value: {type: Number},
                    timestamp: {type: Date}
                },
                min: {
                    value: {type: Number},
                    timestamp: {type: Date}
                }
            }
            // Data itself
            //hourly: [ Schema.Types.Mixed ],
            //minutes: [ [Schema.Types.Mixed] ],
            //seconds: [ [ [Schema.Types.Mixed ] ] ],
            //milliseconds: [ [ [ [Schema.Types.Mixed ] ] ] ],
        });

    /**
     * Generate time series paths
     */
    function init(_options) {
        _.extend(options, _options);
        // Optimize schema performance
        schema.add({ daily: Schema.Types.Mixed});
        schema.add({ hourly: [ Schema.Types.Mixed ]});
        if (options.interval < 3600) {
            //schema.add({minutes: [ {m: [ Schema.Types.Mixed ] }] });
            schema.add({minutes: [ Schema.Types.Mixed ]});
        }
        if (options.interval < 60) {
            //schema.add({seconds: [ {m: [ {s: [ Schema.Types.Mixed ] }]} ]});
            schema.add({seconds: [ Schema.Types.Mixed ]});
        }
        if (options.interval < 1) {
            //schema.add({milliseconds: [ {m: [ {s: [ {ms: [Schema.Types.Mixed ] }] }] }]});
            schema.add({milliseconds: [ Schema.Types.Mixed ]});
        }
    }

    /**
     * Post hook.
     */
    schema.pre('save', function (next) {

        if (this.isNew) {
            this.metadata.interval = options.interval;
            this.statistics.i = 1;
            if (this.latest) {
                this.statistics.min.value = this.latest.value;
                this.statistics.min.timestamp = this.latest.timestamp;
                this.statistics.max.value = this.latest.value;
                this.statistics.max.timestamp = this.latest.timestamp;
                this.statistics.avg = this.latest.value;
            }
        }

        next();
    });

    /**
     * Methods
     */

    var dataFormat = function (timestamp, value, format, ext) {
        switch (format) {
            case '[ms,y]':
                return [ timestamp.getTime(), value ];
            case '[x,y]':
                return [ timestamp, value ];
            case 'hash':
                return _.extend({timestamp: timestamp, value: value }, ext);
        }
    };
    /**
     * Virtual methods
     */
    schema.method('getData', function (interval, from, to, format) {

        var data = [],

            docday = moment.utc([this.day.getFullYear(), this.day.getMonth(), this.day.getDate()]),
            year = docday.get('year'),
            month = docday.get('month'),
            day = docday.get('date'),
            timestamp,
            hour, minute, second, ms;

        if (interval < 1) {
            for (hour in this.milliseconds) {
                if (isNaN(parseInt(hour))) {
                    continue;
                }
                for (minute in this.milliseconds[hour]) {
                    if (!_.isNumber(minute)) {
                        continue;
                    }
                    for (second in this.milliseconds[hour][minute]) {
                        if (!_.isNumber(second)) {
                            continue;
                        }
                        for (ms in this.milliseconds[hour][minute][second]) {
                            if (!_.isNumber(ms)) {
                                continue;
                            }
                            if (this.milliseconds[hour][minute][second][ms]) {
                                //timestamp = new Date(year, month, day, hour, minute, second, ms);
                                timestamp = moment.utc([year, month, day, hour, minute, second, ms]);
                                if (timestamp.diff(from) && to.diff(timestamp)) {
                                    data.push(dataFormat(timestamp, this.milliseconds[hour][minute][second][ms].value, format));
                                }
                            }
                        }
                    }
                }
            }
        } else if (interval < 60) {
            for (hour in this.seconds) {
                if (isNaN(parseInt(hour))) {
                    continue;
                }
                for (minute in this.seconds[hour]) {
                    if (isNaN(parseInt(minute))) {
                        continue;
                    }
                    for (second in this.seconds[hour][minute]) {
                        if (isNaN(parseInt(second))) {
                            continue;
                        }
                        if (this.seconds[hour][minute][second]) {
                            //timestamp = new Date(year, month, day, hour, minute, second);
                            timestamp = moment.utc([year, month, day, hour, minute, second]);

                            if (timestamp.diff(from) > 0 && to.diff(timestamp) > 0) {
                                data.push(dataFormat(timestamp, this.seconds[hour][minute][second].value, format));
                            }
                        }
                    }
                }
            }
        } else if (interval < 3600) {
            for (hour in this.minutes) {
                if (isNaN(parseInt(hour))) {
                    continue;
                }
                for (minute in this.minutes[hour]) {
                    if (isNaN(parseInt(minute))) {
                        continue;
                    }
                    if (this.minutes[hour][minute]) {
                        // timestamp = new Date(year, month, day, hour, minute, 0, 0);
                        timestamp = moment.utc([year, month, day, hour, minute, 0, 0]);
                        if (timestamp.diff(from) > 0 && to.diff(timestamp) > 0) {
                            data.push(dataFormat(timestamp, this.minutes[hour][minute].value, format, {
                                //year: year,  month: month, day: day,
                                hour: hour, minute: minute //metadata: this.minute[hour][minute].metadata
                            }));
                        }
                    }
                }
            }
        } else {
            for (hour in this.hourly) {
                if (isNaN(parseInt(hour))) {
                    continue;
                }
                if (this.hourly[hour] && this.hourly[hour].value) {
                    // timestamp = new Date(year, month, day, hour, 0, 0, 0);
                    timestamp = moment.utc([year, month, day, hour, 0, 0, 0]);
                    if (timestamp.diff(from) > 0 && to.diff(timestamp) > 0) {
                        data.push(dataFormat(timestamp, this.hourly[hour].value, format));
                    }
                }
            }
        }

        return data.reverse();
    });


    schema.method('recalc', function (timestamp, value, cb) {

        var updates = {},
            sum = 0, i = 0,
            thour = timestamp.format('H'),
            tmin = timestamp.format('m'),
            tsec = timestamp.format('s');

        if (this.metadata.interval < 1 && this.millisecond) {
            if (options.verbose) {
                console.log('ms recalc');
            }

            sum = 0;
            i = 0;

            for (var ms in this.millisecond[thour][tmin][tsec]) {
                if (isNaN(parseInt(ms))) {
                    continue;
                }
                if (isNaN(parseInt(this.millisecond[thour][tmin][tsec][ms].value))) {
                    continue;
                }
                sum += this.millisecond[thour][tmin][tsec][ms].value;
                i++;
            }

            if (i <= 0) {
                i = 1;
            }
            if (value && sum === 0) {
                sum = value;
            }

            updates['seconds.' + thour + '.' + tmin + '.' + tsec] = {value: sum / i};
            this.set(updates);
        }
        if (this.metadata.interval < 60 && this.seconds) {
            if (options.verbose) {
                console.log('sec recalc');
            }

            sum = 0;
            i = 0;

            for (var sec in this.seconds[thour][tmin]) {
                if (isNaN(parseInt(sec))) {
                    break;
                }
                if (!this.seconds[thour][tmin][sec]) {
                    continue;
                }
                if (isNaN(parseInt(this.seconds[thour][tmin][sec].value))) {
                    continue;
                }
                sum += this.seconds[thour][tmin][sec].value;
                i++;
            }

            if (i <= 0) {
                i = 1;
            }
            if (value && sum === 0) {
                sum = value;
            }
            updates['minutes.' + thour + '.' + tmin] = {value: sum / i};
            this.set(updates);
        }
        if (this.metadata.interval < 3600 && this.minutes) {
            if (options.verbose) {
                console.log('min recalc');
            }

            sum = 0;
            i = 0;

            for (var min in this.minutes[thour]) {
                if (isNaN(parseInt(min))) {
                    break;
                }
                if (!this.minutes[thour][min]) {
                    continue;
                }
                if (isNaN(parseInt(this.minutes[thour][min].value))) {
                    continue;
                }
                sum += parseInt(this.minutes[thour][tmin].value);
                i = i + 1;
            }

            if (i <= 0) {
                i = 1;
            }
            if (value && sum === 0) {
                sum = value;
            }

            updates['hourly.' + thour ] = { value: sum / i };
            this.set(updates);
        }

        if (i > 0) {
            if (i !== this.statistics.i) {
                updates['statistics.i'] = i;
                this.set(updates);
            }
            if (_.isNumber(sum)) {
                updates['statistics.avg'] = sum / i;
                this.set(updates);
            }
        }

        if (options.verbose) {
            console.log(updates);
        }

        this.save(cb);
    });

    schema.method('minmax', function (timestamp, value) {

        var updates = {}, needToSave = false;

        if (_.isNumber(this.statistics.max.value)) {
            if (value > this.statistics.max.value) {
                updates['statistics.max.timestamp'] = timestamp;
                updates['statistics.max.value'] = value;
                needToSave = true;
            }
        } else {
            updates['statistics.max.timestamp'] = timestamp;
            updates['statistics.max.value'] = value;
            needToSave = true;
        }
        if (_.isNumber(this.statistics.min.value)) {
            if (value < this.statistics.min.value) {
                updates['statistics.min.timestamp'] = timestamp;
                updates['statistics.min.value'] = value;
                needToSave = true;
            }
        } else {
            updates['statistics.min.timestamp'] = timestamp;
            updates['statistics.min.value'] = value;
            needToSave = true;
        }

        if (needToSave) {
            this.set(updates);
            this.save(function (error, ok) {
                if (error) {
                    console.log(error);
                }
            });
        }
    });

    /**
     * Static methods
     */
    schema.static('findMax', function (conditions, callback) {

        var condition = {
            'day': {
                '$gt': conditions.from,
                '$lt': conditions.to
            }
        };

        this.find(condition).limit(1).select('statistics.max').sort({'statistics.max.value': -1}).exec(function (error, doc) {
            if (error) {
                callback(error);
            }
            else if (doc.length === 1) {
                callback(null, doc[0].statistics.max);
            } else {
                callback(null, NaN);
            }
        });
    });

    schema.static('findMin', function (conditions, callback) {
        var condition = {
            'day': {
                '$gte': conditions.from,
                '$lte': conditions.to
            }
        };

        this.find(condition).limit(1).select('statistics.min').sort({'statistics.min.value': 1}).exec(function (error, doc) {
            if (error) {
                callback(error);
            }
            else if (doc.length === 1) {
                callback(null, doc[0].statistics.min);
            } else {
                callback(null, NaN);
            }
        });
    });

    schema.static('findData', function (request, callback) {

        var from = moment(request.from),
            to = moment(request.to);

        if (!request.to) {
            request.to = moment.utc();
        }
        if (!request.dir) {
            request.dir = -1;
        }

        var condition = {
            'day': {
                '$gte': new Date(
                    from.utc().startOf('day').get('year'),
                    from.utc().startOf('day').get('month'),
                    from.utc().startOf('day').get('date'),
                    from.utc().startOf('day').get('hour'),
                    from.utc().startOf('day').get('minute')),
                '$lte': new Date(
                    to.utc().endOf('day').get('year'),
                    to.utc().endOf('day').get('month'),
                    to.utc().endOf('day').get('date'),
                    to.utc().endOf('day').get('hour'),
                    to.utc().endOf('day').get('minute'))
            }
        };


        _.extend(condition, request.condition);

        var select = "metadata statistics latest day minutes hourly";

        if (!_.isNumber(request.interval)) {
            request.interval = 3600;
        }

        if (options.verbose) {
            console.log('request: ', request);
            console.log('condition: ', JSON.stringify(condition));
        }

        this.find(condition).sort({'day': request.dir}).select(select).exec(function (error, docs) {
            if (error) {
                callback(error);
            }
            else {
                if (options.verbose) {
                    console.log('Doc count: ' + docs.length);
                }
                var data = [], i = 1;
                docs.forEach(function (doc) {
                    doc.getData(request.interval, request.from, request.to, request.format).forEach(function (row) {
                        if (i <= request.limit) {
                            data.push(row);
                            i++;
                        } else {
                            callback(null, data);
                        }
                    });
                });
                callback(null, data);
            }
        });
    });


    function getInitializer() {

        var updates = {};

        updates.hourly = [];
        updates.hourly[23] = {};

        if (options.interval < 3600) {
            updates.minutes = [];
            if (options.interval < 60) {
                updates.seconds = [];
            }
            for (var i = 0; i < 24; i++) {
                updates.minutes[i] = [];
                updates.minutes[i][59] = null; //initialize length
                if (options.interval < 60) {
                    updates.seconds[i] = [];
                    for (var j = 0; j < 60; j++) {
                        updates.seconds[i][j] = []; //initialize length
                        updates.seconds[i][j][59] = null; //initialize length
                    }
                }
            }
        }

        return updates;
    }

    function getUpdates(timestamp, value, metadata, first) {

        var updates = {}, set;

        if (options.interval < 1) {
            updates['milliseconds.' + timestamp.get('hour') + '.' + timestamp.get('minute') + '.' + timestamp.get('second') + '.' + timestamp.get('millisecond') + '.value'] = value;
            if (metadata) {
                updates['milliseconds.' + timestamp.get('hour') + '.' + timestamp.get('minute') + '.' + timestamp.get('second') + '.' + timestamp.get('millisecond') + '.metadata'] = metadata;
            }
        } else if (options.interval < 60) {
            set = {value: value};
            if (metadata) {
                set.metadata = metadata;
            }
            updates['seconds.' + timestamp.get('hour') + '.' + timestamp.get('minute') + '.' + timestamp.get('second')] = set;
        } else if (options.interval < 3600) {
            set = { value: value };
            if (metadata) {
                set.metadata = metadata;
            }
            updates['minutes.' + timestamp.get('hour') + '.' + timestamp.get('minute')] = set;
        } else if (options.interval > 0) {
            set = { value: value };
            if (metadata) {
                set.metadata = metadata;
            }
            updates['hourly.' + timestamp.get('hour')] = set;
        }
        //statistics
        updates['updatedAt.date'] = moment.utc().toDate();
        updates['latest.timestamp'] = timestamp.toDate();
        updates['latest.value'] = value;
        updates['$inc'] = {'statistics.i': 1};

        if (metadata) {
            updates['latest.metadata'] = metadata;
        }

        return updates;
    }

    schema.static('recalc', function (timestamp, extraCondition, cb) {

        var day = roundDay(timestamp),
            condition = {'day': day};

        _.extend(condition, extraCondition);

        this.findOne(condition, function (e, doc) {
            if (e) {
                cb(e);
            } else {
                doc.recalc(timestamp, doc.latest.value, cb);
            }
        });
    });

    schema.static('push', function (timestamp, value, metadata, extraCondition, cb) {

        var day = roundDay(timestamp),
            condition = {'day': day};
        _.extend(condition, extraCondition);

        var updates = getUpdates(timestamp, value, metadata),
            self = this;

        if (options.verbose) {
            console.log('\nCondition: ' + JSON.stringify(condition));
            console.log('Update: ' + JSON.stringify(updates));
        }

        this.findOneAndUpdate(condition, updates, function (error, doc) {

            if (error) {
                if (cb) {
                    cb(error);
                }
            } else if (doc) {
                doc.minmax(timestamp, value);
                if (cb) {
                    cb(null, doc);
                }

                if (options.postProcessImmediately) {
                    doc.recalc(timestamp, value);
                }
            } else {
                //console.log('Create new');
                var datainit = getInitializer();
                var newDoc = new self({ day: day});
                newDoc.set(datainit);
                newDoc.set(updates);
                newDoc.save(cb);
            }
        });
    });

    init(args);

    return schema;
};

module.exports = TimeSeriesSchema;