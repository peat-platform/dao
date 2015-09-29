/*
 * DAO
 *
 *
 * Copyright (c) 2013 dconway, dmccarthy
 * Licensed under the MIT license.
 */

'use strict';

var zmq                 = require('zmq');
var zmqM2Node           = require('m2nodehandler');
var helper              = require('./CBasehelper.js');
var perms_prop_worker   = require('./perms_propogator_worker');
var subscriptionHandler = require('./subscriptions');
var HTTPStatus          = require('http-status');
var loglet              = require('cloudlet-utils').loglet;


var mongrel_handlers = {};

var getMongrelSink = function(mongrel_sink) {
   return (
      function(mongrel_sink) {
         if (undefined === mongrel_sink || "" === mongrel_sink){
            return {send:function(){}};
         }

         if (!mongrel_handlers[mongrel_sink.spec]) {
            mongrel_handlers[mongrel_sink.spec] = zmqM2Node.sender(mongrel_sink);
         }

         return mongrel_handlers[mongrel_sink.spec];

      }(mongrel_sink, mongrel_handlers)
      );
};


var dao = function(config) {

   var logger = loglet.createLogger(config.logging.name, config.logging.log_level, config.logging.log_file_name)

   helper.init(logger, config.tracklet_worker, config.perms_propagator_f, config.couchbase_cluster, config.couchbase_n1ql);
   subscriptionHandler.init((config));

   zmqM2Node.receiver(config.dao_sink, null, function(msg) {

      var mongSink = getMongrelSink(msg.mongrel_sink);

      helper.evaluateMessage(msg, function (err, results) {
         if(err) {
            logger.error(err);
         }
         var httpStatusCode = HTTPStatus.OK;
         var resp           = {};
         var headers        = zmqM2Node.standard_headers.json;

         if (1 === Object.keys(results).length) {
            resp             = results[0][0];
            httpStatusCode   = results[0][1];
            if(undefined !== results[0][2]){
               headers = results[0][2];
            }
         } else {
            httpStatusCode   = results[0][1];
            headers          = results[0][2];
            resp             = [];
            for ( var i in results ) {
               resp[i] = results[i][0];
            }

         }

         for (var i=0; i < msg.clients.length; i++) {
            var client = msg.clients[i];
            if (resp !== undefined) {
               subscriptionHandler.checkSubscription(msg, httpStatusCode, resp);
            }
            mongSink.send(client.uuid, client.connId, httpStatusCode, headers, resp);
         }
      });
   });
};




dao['perms_prop_worker'] = perms_prop_worker.init;

module.exports = dao;