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

var init = function (logger_params, callback) {

   this.logger = openiLogger(logger_params)

   if (flag) {
      return
   }
   nano.db.destroy('db', function () {
      nano.db.create('db', function () {
         flag = true;
         callback()
      })
   })

}


var putAction = function (msg, callback) {
   var db = nano.use('db');
   //return insertDocument(msg.data,  msg.name[1], msg)
   db.insert(msg.data, msg.name[1], function (err) {
      var status = zmq.status.OK_200
      var body = {}
      if (err) {
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

var handleTypeParameters = function (db_body, msg) {
   var query  = msg.query
   var params = {}
   query.replace(/([^=&]+)=([^&]*)/gi, function (m, key, value) {
      params[key] = value
   })
   var object = null
   var doc, data, key

   if (typeof params['oids'] !== 'undefined' && params['oids'] === 'true') {
      doc    = db_body['types']
      object = doc[msg.name[3]]
   }
   else if (typeof params['blob'] !== 'undefined' && params['blob'] === 'false') {
      doc    = db_body['types']
      object = doc[msg.name[3]]
      data   = db_body['data']
      for (key in object) {
         if (data.hasOwnProperty(object[key])) {
            object[key] = data[object[key]]
         }
      }
   }
   else if (typeof params['blob'] !== 'undefined' && params['blob'] === 'true') {

      doc          = db_body['types']
      object       = doc[msg.name[3]]
      var blobs    = db_body['blobs']
      data         = db_body['data']
      var blobkeys = Object.keys(blobs)

      for (key in object) {
         if (typeof key !== 'undefined') {
            if (data.hasOwnProperty(object[key]) && (blobkeys.indexOf(data[object[key]]) !== -1)) {
               object[key] = blobs[data[object[key]]]
            }
         }
      }
   }

   return object
}


var getAction = function (msg, callback) {

   var db = nano.use('db')
   var response

   db.get(msg.name[1], {revs_info: false}, function (err, db_body) {
      var status = zmq.status.OK_200
      var body   = {}
      var doc, object

      switch(msg.name.length) {
      case 2:
         object = db_body[msg.name[0]]
         break
      case 3:
         doc    = db_body[msg.name[0]]
         object = doc[msg.name[2]]
         break
      case 4:
         if (msg.name[2] === 'type') {
            if (typeof msg.query !== 'undefined') {
               object = handleTypeParameters(db_body, msg)
            }
            else {
               doc    = db_body['types']
               object = doc[msg.name[3]]
            }
         }
         else {
            doc       = db_body['data']
            var field = doc[msg.name[2]]
            object    = field[msg.name[3]]
         }
         break
      default:
         object = db_body
         break
      }
      if (err) {
         status = zmq.status.INTERNAL_SERVER_ERROR_500
         body['result'] = 'failure'
         body['error']  = 'Error getting data from datastore'
      }
      else {
         body['result'] = 'success'
         body['value']  = object
      }

      response = zmq.Response(status, zmq.header_json, body)
      callback(response)
   })

}


var evaluateMethod = function (msg, callback) {

   dbc.hasMember(msg, 'action')
   dbc.hasMember(msg, 'name'  )

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



//var insertDocument = function (data, name, msg) {
//   var db = nano.use('db');
//   db.insert(data, name, function (err) {
//      var status = zmq.status.OK_200
//      var body   = {}
//      if (err) {
//         status = zmq.status.INTERNAL_SERVER_ERROR_500
//         body['result'] = 'failure'
//         body['error']  = 'Error: ' + err['Error'];
//      }
//      else {
//         body['result'] = 'success'
//      }
//      return zmq.Response(status, zmq.header_json, body)
//   })
//}


module.exports.init = init
module.exports.evaluateMethod = evaluateMethod