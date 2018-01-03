'use strict';

/**
 *  Model capsel for mongoose-timeseries
 *
 *  Author: Jussi Vatjus-Anttila
 */

/** Module dependencies. */
var _ = require("underscore"),
    mongoose = require('mongoose'),
    /** import schema */
    TimeSeriesSchema = require('./schema');

var TimeSeriesModel = function (connection, collection, options) {

    var model,
        schema;

    /**
     * Methods
     */

    /**
     * Model initialization
     */
    function init(connection, collection, options) {

        if (connection.modelNames().indexOf(collection) >= 0) {
            model = connection.model(collection);
        } else {
            schema = new TimeSeriesSchema(options);
            connection.model(collection, schema);
            model = connection.model(collection);
        }
    }

    /**
     * Push new value to collection
     */
    var push = function push(timestamp, value, extraCondition, cb) {
        model.push(timestamp, value, extraCondition, cb);
    };
    /**
     *  Find data of given period
     */
    var findData = function (options, cb) {
        model.findData(options, cb);
    };
    /**
     * Find Max value of given period
     */
    var findMax = function (options, cb) {
        model.findMax(options, cb);
    };
    /**
     * Find Min value of given period
     */
    var findMin = function (options, cb) {
        model.findMin(options, cb);
    };

    var getModel = function () {
        return model;
    };
    var getSchema = function () {
        return schema;
    };

    init(connection, collection, options);

    /* Return model api */
    return {
        getModel: getModel,
        getSchema: getSchema,
        push: push,
        findData: findData,
        findMax: findMax,
        findMin: findMin,
        model: model,
        findRaw: model.findRaw.bind(model)
    };
};

module.exports = TimeSeriesModel;
