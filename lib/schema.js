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
        paths: { value: { type: 'number' }, metadata: { type: Mixed } }
    },
        roundDay = function (d) {
            return moment({ year: d.get('year'), month: d.get('month'), day: d.get('date') }).startOf('day');
        },


        // statistics = new Schema({
        //     avg: { type: Number },
        //     max: { type: Number },
        //     min: { type: Number },
        //     count: { type: Number, default: 0 },
        //     sum: { type: Number },
        //     sumSquare: { type: Number }
        // }),

    schema = new Schema({
        day: { type: Date, index: true, required: true },
        metadata: {
            interval: { type: Number }
        },
        latest: {
            timestamp: { type: Date },
            value: { type: Number, default: 0 },
            metadata: { type: Mixed }
        },
        createdAt: {
            date: { type: Date, default: Date },
            user: { type: String }
        },
        updatedAt: {
            date: { type: Date },
            user: { type: String }
        },
        statistics: {
                avg: { type: Number },
                max: { type: Number },
                min: { type: Number },
                count: { type: Number, default: 0 },
                sum: { type: Number },
                sumSquare: { type: Number }
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
        schema.add({ hourly: [Schema.Types.Mixed] });
        if (options.interval < 3600) {
            //schema.add({minutes: [ {m: [ Schema.Types.Mixed ] }] });
            schema.add({ minutes: [Schema.Types.Mixed] });
        }
        if (options.interval < 60) {
            //schema.add({seconds: [ {m: [ {s: [ Schema.Types.Mixed ] }]} ]});
            schema.add({ seconds: [Schema.Types.Mixed] });
        }
        if (options.interval < 1) {
            //schema.add({milliseconds: [ {m: [ {s: [ {ms: [Schema.Types.Mixed ] }] }] }]});
            schema.add({ milliseconds: [Schema.Types.Mixed] });
        }
        getInitializer();
    }

    /**
     * Post hook.
     */
    schema.pre('save', function (next) {

        if (this.isNew) {
            this.metadata.interval = options.interval;
            // this.statistics.count = 1;
            // if (this.latest) {
            //     this.statistics.min = this.latest.value;
            //     this.statistics.max = this.latest.value;
            //     this.statistics.sum = this.latest.value;
            //     this.statistics.sumSquare = this.latest.value * this.latest.value;
            // }
        }

        next();
    });

    schema.post('find', function (docs) {
        docs.forEach(function (doc) {
            if (doc.statistics.sum)
                doc.statistics.avg = doc.statistics.sum / doc.statistics.count;
        });
    });

    /**
     * Methods
     */

    var dataFormat = function (timestamp, value, format, ext) {
        switch (format) {
            case '[ms,y]':
                return [timestamp.valueOf(), value];
            case '[x,y]':
                return [timestamp, value];
            case 'hash':
                return _.extend({ timestamp: timestamp, value: value }, ext);
        }
    };

    var getUTCDateFromMoment = function (time) {
        return new Date(
            time.get('year'),
            time.get('month'),
            time.get('date'),
            time.get('hour'),
            time.get('minute'),
            time.get('second'),
            time.get('millisecond')
        );
    };
    /**
     * Virtual methods
     */
    schema.method('getData', function (interval, from, to, format, reverse) {

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
                                if (timestamp.diff(from) && to.diff(timestamp)) { //??
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

                            if (timestamp.diff(from) >= 0 && to.diff(timestamp) >= 0) {
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
                        if (timestamp.diff(from) >= 0 && to.diff(timestamp) >= 0) {
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
                if (this.hourly[hour] && this.hourly[hour].count) {
                    // timestamp = new Date(year, month, day, hour, 0, 0, 0);
                    timestamp = moment.utc([year, month, day, hour, 0, 0, 0]);
                    if (timestamp.diff(from) >= 0 && to.diff(timestamp) >= 0) {
                        data.push(dataFormat(timestamp, this.hourly[hour].sum / this.hourly[hour].count, format));
                    }
                }
            }
        }

        if (reverse) {
            return data.reverse();
        }
        return data;
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

            updates['seconds.' + thour + '.' + tmin + '.' + tsec] = { value: sum / i };
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
            updates['minutes.' + thour + '.' + tmin] = { value: sum / i };
            this.set(updates);
        }
        // if (this.metadata.interval < 3600 && this.minutes) {
        //     if (options.verbose) {
        //         console.log('min recalc');
        //     }

        //     sum = 0;
        //     i = 0;

        //     for (var min in this.minutes[thour]) {
        //         if (isNaN(parseInt(min))) {
        //             break;
        //         }
        //         if (!this.minutes[thour][min]) {
        //             continue;
        //         }
        //         if (isNaN(parseInt(this.minutes[thour][min].value))) {
        //             continue;
        //         }
        //         sum += parseInt(this.minutes[thour][tmin].value);
        //         i = i + 1;
        //     }

        //     if (i <= 0) {
        //         i = 1;
        //     }
        //     if (value && sum === 0) {
        //         sum = value;
        //     }

        //     //updates['hourly.' + thour] = { value: sum / i };
        //     this.set(updates);
        // }

        if (options.verbose) {
            console.log(updates);
        }

        this.save(cb);
    });

    /**
     * Static methods
     */
    schema.static('findMax', function (conditions, callback) {

        var condition = {
            'day': {
                '$gte': getUTCDateFromMoment(conditions.from),
                '$lte': getUTCDateFromMoment(conditions.to)
            }
        };

        this.find(condition).limit(1).select('statistics.max').sort({ 'statistics.max': -1 }).exec(function (error, doc) {
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
                '$gte': getUTCDateFromMoment(conditions.from),
                '$lte': getUTCDateFromMoment(conditions.to)
            }
        };

        this.find(condition).limit(1).select('statistics.min').sort({ 'statistics.min': 1 }).exec(function (error, doc) {
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
    
    schema.static('findRaw', function (request, callback) {

        var select,
            from = moment(request.from),
            to = moment(request.to);

        request.limit = request.limit || 0;

        if (request.interval < 60) { /* seconds */
            select = "metadata statistics latest day seconds";
        } else if (request.interval < 3600) { /* minutes */
            select = "metadata statistics latest day minutes";
        } else {
            select = "metadata statistics latest day hourly";
        }

        if (!request.to) {
            request.to = moment.utc();
        }
        request.reverse = request.reverse || false;
        if (request.reverse) {
            request.dir = -1;
        }
        else {
            request.dir = 1;
        }


        var condition = {
            'day': {
                '$gte': getUTCDateFromMoment(from.utc().startOf('day')),
                '$lte': getUTCDateFromMoment(to.utc().endOf('day')),
            }
        };


        _.extend(condition, request.condition);

        if (!_.isNumber(request.interval)) {
            request.interval = 3600;
        }

        if (options.verbose) {
            console.log('request: ', request);
            console.log('condition: ', JSON.stringify(condition));
        }

        this.find(condition).sort({ 'day': request.dir })./*select(select).*/exec(callback);
    });

    schema.static('findData', function (request, callback) {

        // var select,
        //     from = moment(request.from),
        //     to = moment(request.to);

        // request.limit = request.limit || 0;

        // if (request.interval < 60) { /* seconds */
        //     select = "metadata statistics latest day seconds";
        // } else if (request.interval < 3600) { /* minutes */
        //     select = "metadata statistics latest day minutes";
        // } else {
        //     select = "metadata statistics latest day hourly";
        // }

        // if (!request.to) {
        //     request.to = moment.utc();
        // }
        // request.reverse = request.reverse || false;
        // if (request.reverse) {
        //     request.dir = -1;
        // }
        // else {
        //     request.dir = 1;
        // }


        // var condition = {
        //     'day': {
        //         '$gte': getUTCDateFromMoment(from.utc().startOf('day')),
        //         // new Date(
        //         //     from.utc().startOf('day').get('year'),
        //         //     from.utc().startOf('day').get('month'),
        //         //     from.utc().startOf('day').get('date'),
        //         //     from.utc().startOf('day').get('hour'),
        //         //     from.utc().startOf('day').get('minute')),
        //         '$lte': getUTCDateFromMoment(to.utc().endOf('day')),
        //         //  new Date(
        //         //     to.utc().endOf('day').get('year'),
        //         //     to.utc().endOf('day').get('month'),
        //         //     to.utc().endOf('day').get('date'),
        //         //     to.utc().endOf('day').get('hour'),
        //         //     to.utc().endOf('day').get('minute'))
        //     }
        // };


        // _.extend(condition, request.condition);

        // if (!_.isNumber(request.interval)) {
        //     request.interval = 3600;
        // }

        // if (options.verbose) {
        //     console.log('request: ', request);
        //     console.log('condition: ', JSON.stringify(condition));
        // }

        // this.find(condition).sort({ 'day': request.dir }).select(select).exec(
        this.findRaw(request, function (error, docs) {
            if (error) {
                callback(error);
            }
            else {
                if (options.verbose) {
                    console.log('Doc count: ' + docs.length);
                }
                var data = [], i = 1;
                docs.forEach(function (doc) {
                    doc.getData(request.interval, request.from, request.to, request.format, request.reverse).forEach(function (row) {
                        if (!request.limit || i <= request.limit) {
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


    var initializer = null;
    function getInitializer() {

        if(initializer) 
        {
            return initializer;
        }
        initializer = {};


        // updates.hourly = [];
        // updates.hourly[23] = {};

        var statisticsStruct = {
            count: 0,
            min: Number.POSITIVE_INFINITY,
            max: Number.NEGATIVE_INFINITY,
            sum: 0,
            sumSquare: 0
        };

        initializer.statistics = statisticsStruct;// _.clone(statisticsStruct);
        initializer.hourly = new Array(24);
        for (let index = 0; index < initializer.hourly.length; index++) {
            initializer.hourly[index] = statisticsStruct;//_.clone(statisticsStruct);
        }

        if (options.interval < 3600) {
            initializer.minutes = new Array(24);

            var minuteInit = new Array(60);
            for (var i = 0; i < minuteInit.length; i++) {
                minuteInit[i] = statisticsStruct;
            }
            
            for (var i = 0; i < initializer.minutes.length; i++) {
                initializer.minutes[i] = minuteInit;
            }


            if (options.interval < 60) {
                initializer.seconds = [];
            }
            for (var i = 0; i < 24; i++) {
                // updates.minutes[i] = [];
                // updates.minutes[i][59] = null; //initialize length

                //seconds
                if (options.interval < 60) {
                    initializer.seconds[i] = [];
                    for (var j = 0; j < 60; j++) {
                        initializer.seconds[i][j] = []; //initialize length
                        initializer.seconds[i][j][59] = null; //initialize length
                    }
                }
            }
        }

        return initializer;
    }

    function getUpdates(timestamp, value, first) {

        var updates = {}, set;
        var inc = {};
        var min = {};
        var max = {};

        if (options.interval < 1) {
            updates['milliseconds.' + timestamp.get('hour') + '.' + timestamp.get('minute') + '.' + timestamp.get('second') + '.' + timestamp.get('millisecond') + '.value'] = value;
        } else if (options.interval < 60) {
            set = { value: value };
            updates['seconds.' + timestamp.get('hour') + '.' + timestamp.get('minute') + '.' + timestamp.get('second')] = set;
        } else if (options.interval < 3600) {
            //  set = { value: value };
            //  updates['minutes.' + timestamp.get('hour') + '.' + timestamp.get('minute')] = set;
            var minutePath = 'minutes.' + timestamp.get('hour') + '.' + timestamp.get('minute');
            inc[minutePath + '.count'] = 1;
            inc[minutePath + '.sum'] = value;
            inc[minutePath + '.sumSquare'] = value * value;
            min[minutePath + '.min'] = value;
            max[minutePath + '.max'] = value;
        } 
        
        if (options.interval > 0) {
            inc['hourly.' + timestamp.get('hour') + '.count'] = 1;
            inc['hourly.' + timestamp.get('hour') + '.sum'] = value;
            inc['hourly.' + timestamp.get('hour') + '.sumSquare'] = value * value;
            min['hourly.' + timestamp.get('hour') + '.min'] = value;
            max['hourly.' + timestamp.get('hour') + '.max'] = value;
        }
        //statistics
        updates['updatedAt.date'] = moment.utc().toDate();
        updates['latest.timestamp'] = timestamp.toDate();
        updates['latest.value'] = value;


        inc['statistics.count'] = 1;
        inc['statistics.sum'] = value;
        inc['statistics.sumSquare'] = value * value;
        updates['$inc'] = inc;

        // updates['$inc'] = {
        //     'statistics.count': 1,
        //     'statistics.sum': value,
        //     'statistics.sumSquare': value * value
        // };

        min['statistics.min'] = value;
        updates['$min'] = min;

        max['statistics.max'] = value;
        updates['$max'] = max;

        return updates;
    }

    schema.static('recalc', function (timestamp, extraCondition, cb) {

        if (options.interval < 60) {
            var day = roundDay(timestamp),
                condition = { 'day': day };

            _.extend(condition, extraCondition);

            this.findOne(condition, function (e, doc) {
                if (e) {
                    cb(e);
                } else {
                    doc.recalc(timestamp, doc.latest.value, cb);
                }
            });
        }
        else if (cb) {
            cb();
        }
    });

    schema.static('push', function (timestamp, value, extraCondition, cb) {

        var that = this;
        var day = roundDay(timestamp);
        var condition = { 'day': day };
        _.extend(condition, extraCondition);

        var updates = getUpdates(timestamp, value),
            self = this;

        if (options.verbose) {
            console.log('\nCondition: ' + JSON.stringify(condition));
            console.log('Update: ' + JSON.stringify(updates));
        }

 /*new: true*/
        this.findOneAndUpdate(condition, updates, { new: true, projection: {day: true, latest: true, statistics: true} }, function (error, doc) {

            if (error) {
                if (cb) {
                    cb(error);
                }
            } else if (doc) {
                if (cb) {
                    cb(null, doc);
                }

                // if (options.postProcessImmediately) {
                //     doc.recalc(timestamp, value);
                // }
            } else {
                //console.log('Create new');
                var timeStart = Date.now();
                var datainit = getInitializer();
                var newDoc = new self({ day: day });
                newDoc.set(datainit);
                newDoc.save(function (error, doc) {
                    var time = Date.now() - timeStart;
                    // console.log("Insert took: " + time + " ms");
                    if (error) {
                        if (cb) {
                            cb(error);
                        } 
                    }
                    else {
                        that.findOneAndUpdate(condition, updates, { new: true }, function (error, doc) {                            
                            if (cb && doc) {
                                cb(error,doc);
                            }
                        });
                    }                    
                });
            }
        });
    });

    init(args);

    return schema;
};

module.exports = TimeSeriesSchema;
