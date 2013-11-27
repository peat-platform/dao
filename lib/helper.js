/*
 * openi_data_api
 * openi-ict.eu
 *
 * Copyright (c) 2013 dconway
 * Licensed under the MIT license.
 */

'use strict';

var nano        = require('nano')('http://localhost:5984');
var dbc         = require('dbc');
var openiLogger = require('openi-logger')
var zmq         = require('m2nodehandler')
var flag        = false;
var logger      = null;

var init = function (logger_params, callback) {

   logger = openiLogger(logger_params)

   if (flag) {
      return
   }
   nano.db.destroy('dmccarthy', function () {
      nano.db.create('dmccarthy', function () {
         flag = true;
         callback()
      })
   })

}


var putAction = function (msg, callback) {

   var db = nano.use(msg.cloudlet);
   //return insertDocument(msg.data,  msg.name[1], msg)


   db.insert(msg.object_data, msg.object_name, function (err) {

      var status = zmq.status.OK_200
      var body   = {}
      if (err) {
         logger.log('debug', err )
         status = zmq.status.INTERNAL_SERVER_ERROR_500
         body['result'] = 'failure'
         body['error']  = 'Error: ' + err['Error'];
      }
      else {
         body['result'] = 'success'
      }
      callback(zmq.Response(status, zmq.header_json, body))
   })
}


var getAction = function (msg, callback) {

   var db = nano.use(msg.cloudlet)


   db.get(msg.object_name, {revs_info: false}, function (err, db_body) {
      var status = zmq.status.OK_200
      var body   = {}

      console.log(db_body)

      var object = db_body

      if (err) {
         status = zmq.status.INTERNAL_SERVER_ERROR_500
         body['result'] = 'failure'
         body['error']  = 'Error getting data from datastore'
      }
      else {
         body['result'] = 'success'
         body['value']  = object
      }

      var response = zmq.Response(status, zmq.header_json, body)
      callback(response)
   })

}


var evaluateMethod = function (msg, callback) {

   logger.log('debug', msg)

   dbc.hasMember(msg, 'action')
   dbc.hasMember(msg, 'object_name'  )

   if (msg.action === 'PUT') {
      putAction(msg, function (returned) {
         callback(returned)
      })
   }
   else if (msg.action === 'GET') {
      getAction(msg, function (returned) {
         callback(returned)
      })
   }
   else {
      var body = {
         error: 'Action parameter is required'
      }
      callback(zmq.Response(zmq.status.BAD_REQUEST_400, zmq.header_json, body))
   }
}



module.exports.init           = init
module.exports.evaluateMethod = evaluateMethod