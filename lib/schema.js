/**
 * Module dependencies.
 */
var _ = require("underscore");
var mongoose = require('mongoose')
    , Schema = mongoose.Schema
    , Mixed = mongoose.Schema.Types.Mixed;

var TimeSeriesSchema = function (args) {
    /*
     allowed interval values (seconds):
     0.001, 0.002, 0.005, 0.01, 0,02, 0,05, 0.1, 0.2, 0.5,       //milliseconds
     1, 2, 5, 10, 30,  60,                                       //seconds
     120 (2min), 300 (5min), 600 (10min), 1800(30min), 3600(1h)  //minutes
     */

    var options = {
        actor: 0,
        interval: 60,  // seconds
        millisecond: false,
        verbose: false,
        postProcessImmediately: true,
        paths: {value: {type: 'number'}, metadata: {type: Mixed}}
    };

    var roundDay = function (d) {
        return new Date(d.getFullYear(),
            d.getMonth(),
            d.getDate());
    };

    /**
     * Schema definition
     */
    var schema = new Schema({
        day: {type: Date, index: true, required: true},
        actor: {type: Number, required: true, index: true},
        metadata: {
            interval: {type: Number},
        },
        latest: {
            timestamp: {type: Date},
            value: {type: Number, default: 0},
            metadata: {type: Mixed},
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
        },
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
        //console.log(this);
        if (this.isNew) {
            //console.log('saving new..');
            this.metadata.interval = options.interval;
            this.statistics.i = 1;
            if (this.latest) {
                this.statistics.min.value = this.latest.value;
                this.statistics.min.timestamp = this.latest.timestamp;
                this.statistics.max.value = this.latest.value;
                this.statistics.max.timestamp = this.latest.timestamp;
                this.statistics.avg = this.latest.value;
            }
        } else {
            //console.log('updating old..');
        }
        next();
    });

    /**
     * Methods
     */

    var dataFormat = function (timestamp, value, format, ext) {
        switch (format) {
            case('[ms,y]'):
                return [ timestamp.getTime(), value ];
            case('[x,y]'):
                return [ timestamp, value ];
            default:
            case('hash'):
                return _.extend({ timestamp: timestamp, value: value }, ext);

        }
    };
    /**
     * Virtual methods
     */
    schema.method('getData', function (interval, format) {
        var data = [];
        var year = this.day.getFullYear();
        var month = this.day.getMonth();
        var day = this.day.getDate();
        if (interval < 1) {
            for (var hour in this.milliseconds) {
                if (isNaN(parseInt(hour)))continue;
                for (var minute in this.milliseconds[hour]) {
                    if (!_.isNumber(minute))continue;
                    for (var second in this.milliseconds[hour][minute]) {
                        if (!_.isNumber(second))continue;
                        for (var ms in this.milliseconds[hour][minute][second]) {
                            if (!_.isNumber(ms))continue;
                            if (this.milliseconds[hour][minute][second][millisecond]) {
                                var timestamp = new Date(year, month, day, hour, minute, second, millisecond);
                                data.push(dataFormat(timestamp, this.milliseconds[hour][minute][second][millisecond], format));
                            }
                        }
                    }
                }
            }
        } else if (interval < 60) {
            for (var hour in this.seconds) {
                if (isNaN(parseInt(hour)))continue;
                for (var minute in this.seconds[hour]) {
                    if (isNaN(parseInt(minute)))continue;
                    for (var second in this.seconds[hour][minute]) {
                        if (isNaN(parseInt(second)))continue;
                        if (this.seconds[hour][minute][second]) {
                            var timestamp = new Date(year, month, day, hour, minute, second);
                            data.push(dataFormat(timestamp, this.seconds[hour][minute][second].value, format));
                        }
                    }
                }
            }
        } else if (interval < 3600) {
            for (var hour in this.minutes) {
                if (isNaN(parseInt(hour)))continue;
                for (var minute in this.minutes[hour]) {
                    if (isNaN(parseInt(minute)))continue;
                    if (this.minutes[hour][minute]) {
                        var timestamp = new Date(year, month, day, hour, minute, 0, 0);
                        data.push(dataFormat(timestamp, this.minutes[hour][minute].value, format, {
                            //year: year,  month: month, day: day,
                            hour: hour, minute: minute //metadata: this.minute[hour][minute].metadata
                        }));
                    }
                }
            }
        } else
            for (var hour in this.hourly) {
                if (isNaN(parseInt(hour)))continue;
                if (this.hourly[hour] && this.hourly[hour].value) {
                    var timestamp = new Date(year, month, day, hour, 0, 0, 0);
                    data.push(dataFormat(timestamp, this.hourly[hour].value, format));
                }
            }
        return data;
    });


    schema.method('recalc', function (timestamp, value, cb) {
        var updates = {};
        var sum = 0, i = 0,
            hour, min, sec, ms;
        if (this.metadata.interval < 1 && this.millisecond) {
            if (options.verbose)console.log('ms recalc');
            sum = 0;
            i = 0;
            for (hour in this.millisecond) {
                if (isNaN(parseInt(hour)))continue;
                for (min in this.millisecond[hour]) {
                    if (isNaN(parseInt(min)))continue;
                    for (sec in this.millisecond[hour][min]) {
                        if (isNaN(parseInt(sec)))continue;
                        for (ms in this.millisecond[hour][min][sec]) {
                            if (isNaN(parseInt(ms)))continue;
                            if (isNaN(parseInt(this.millisecond[hour][min][sec][ms].value)))continue;
                            sum += this.millisecond[hour][min][sec][ms].value;
                            i++;
                        }
                    }
                }
            }
            if (i <= 0) i = 1;
            if (value && sum == 0)sum = value;
            updates['seconds.' + timestamp.getHours() + '.' + timestamp.getMinutes() + '.value'] = sum / i;
            this.set(updates);
        }
        if (this.metadata.interval < 60 && this.seconds) {
            if (options.verbose)console.log('s recalc');
            sum = 0;
            i = 0;

            for (hour in this.seconds) {
                if (isNaN(parseInt(hour)))break;
                for (min in this.seconds[hour]) {
                    if (isNaN(parseInt(min)))break;
                    for (sec in this.seconds[hour][min]) {
                        if (isNaN(parseInt(sec)))break;
                        if (!this.seconds[hour][min][sec])continue;
                        if (isNaN(parseInt(this.seconds[hour][min][sec].value)))continue;
                        sum += this.seconds[hour][min][sec].value;
                        i++;
                    }
                }
            }
            if (i <= 0) i = 1;
            if (value && sum == 0)sum = value;
            updates['minutes.' + timestamp.getHours() + '.' + timestamp.getMinutes()] = {value: sum / i};
            this.set(updates);
        }
        if (this.metadata.interval < 3600 && this.minutes) {
            if (options.verbose)console.log('min recalc');
            sum = 0;
            i = 0;
            for (var hour in this.minutes) {
                if (isNaN(parseInt(hour)))break;
                for (var min in this.minutes[hour]) {
                    if (isNaN(parseInt(min)))break;
                    if (!this.minutes[hour][min])continue;
                    if (isNaN(parseInt(this.minutes[hour][min].value)))continue;
                    sum += parseInt(this.minutes[hour][min].value);
                    i++;
                }
            }
            if (i <= 0) i = 1;
            if (value && sum == 0)sum = value;
            updates['hourly.' + timestamp.getHours() ] = { value: sum / i };
            this.set(updates);
        }
        if (this.metadata.interval >= 3600 && this.hourly) {
            sum = 0;
            i = 0;
            for (hour in this.hourly) {
                if (this.hourly[hour] === null || isNaN(parseInt(hour)))break;
                if (!this.hourly[hour])continue;
                if (isNaN(parseInt(this.hourly[hour].value)))continue;
                sum += parseInt(this.hourly[hour].value);
                i++;
            }
            if (i <= 0) i = 1;
        }

        if (i > 0) {
            if (i != this.statistics.i) {
                updates['statistics.i'] = i;
                this.set(updates);
            }
            if (_.isNumber(sum)) {
                updates['statistics.avg'] = sum / i;
                this.set(updates);
            }
        }
        if (options.verbose)console.log(updates);
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
                if (error) console.log(error);
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
        //console.log('findMax: '+JSON.stringify(condition));
        this.find(condition).limit(1).select('statistics.max').sort({'statistics.max.value': -1}).exec(function (error, doc) {
            if (error) callback(error);
            else if (doc.length == 1) {
                callback(null, doc[0].statistics.max);
            } else callback(null, NaN);
        });
    });
    schema.static('findMin', function (conditions, callback) {
        var condition = {
            'day': {
                '$gt': conditions.from,
                '$lt': conditions.to
            }
        };
        //console.log('findMin: '+JSON.stringify(condition));
        this.find(condition).limit(1).select('statistics.min').sort({'statistics.min.value': 1}).exec(function (error, doc) {
            if (error) callback(error);
            else if (doc.length == 1) {
                callback(null, doc[0].statistics.min);
            } else callback(null, NaN);
        });
    });

    schema.static('findData', function (request, callback) {
            if (!request.to) request.to = new Date();
            if (!request.dir) request.dir = 1;

            var condition = {
                'day': {
                    '$gt': request.from,
                    '$lt': request.to
                }
            };
            _.extend(condition, request.condition);

            var select = "metadata statistics latest day seconds minutes hourly";

            if (!_.isNumber(request.interval)) {
                request.interval = 3600;
            }

            if (options.verbose)console.log('request: ', request);
            if (options.verbose)console.log('condition: ', JSON.stringify(condition));
            this.find(condition).sort({'day': request.dir}).select(select).exec(function (error, docs) {
                if (error) {
                    callback(error);
                }
                else {
                    if (options.verbose)console.log('Doc count: ' + docs.length);
                    var data = [], i = 1;
                    docs.forEach(function (doc) {
                        doc.getData(request.interval, request.format).forEach(function (row) {
                            if (i <= request.limit)
                                data.push(row);
                            i++;
                        });
                    });
                    callback(null, data);
                }
            });
        }
    )
    ;


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

        var updates = {};
        if (options.interval < 1) {
            updates['milliseconds.' + timestamp.getHours() + '.' + timestamp.getMinutes() + '.' + timestamp.getSeconds() + '.' + timestamp.getMilliseconds() + '.value'] = value;
            if (metadata) updates['milliseconds.' + timestamp.getHours() + '.' + timestamp.getMinutes() + '.' + timestamp.getSeconds() + '.' + timestamp.getMilliseconds() + '.metadata'] = metadata;
        } else if (options.interval < 60) {
            var set = {value: value};
            if (metadata) set.metadata = metadata;
            updates['seconds.' + timestamp.getHours() + '.' + timestamp.getMinutes() + '.' + timestamp.getSeconds()] = set
        } else if (options.interval < 3600) {
            var set = { value: value };
            if (metadata) set.metadata = metadata;
            updates['minutes.' + timestamp.getHours() + '.' + timestamp.getMinutes()] = set;
        } else if (options.interval > 0) {
            var set = { value: value };
            if (metadata) set.metadata = metadata;
            updates['hourly.' + timestamp.getHours()] = set;
        }
        //statistics
        updates['updatedAt.date'] = new Date();
        updates['latest.timestamp'] = timestamp;
        updates['latest.value'] = value;
        updates['$inc'] = {'statistics.i': 1};
        if (metadata)updates['latest.metadata'] = metadata;
        return updates;
    }

    schema.static('recalc', function (timestamp, extraCondition, cb) {
        var day = roundDay(timestamp);
        var condition = {'day': day};
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
        var day = roundDay(timestamp);
        var condition = {'day': day};
        _.extend(condition, extraCondition);
        var updates = getUpdates(timestamp, value, metadata);
        var self = this;
        if (options.verbose)console.log('\nCond: ' + JSON.stringify(condition));
        if (options.verbose)console.log('Upda: ' + JSON.stringify(updates));
        this.findOneAndUpdate(condition, updates, function (error, doc) {
            if (error) {
                if (cb)cb(error);
            } else if (doc) {
                //console.log('Updated -> calc stats');
                doc.minmax(timestamp, value);
                if (cb)cb(null, doc);

                if (options.postProcessImmediately) {
                    doc.recalc(timestamp, value);
                }

            } else {
                //console.log('Create new');
                var datainit = getInitializer();
                var newDoc = new self({ day: day, actor: options.actor });
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
