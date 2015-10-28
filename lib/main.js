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
var cloudlet_utils      = require('cloudlet-utils').cloudlet_utils;
var crypto              = require('crypto');


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


var _hash = function(p, regex){

   var cloudletPattern = new RegExp(regex);
   var result          = cloudletPattern.exec(p);

   if (null !== result) {
      for (var i = 0; i < result.length; i++) {
         var cid = result[i]
         var hcid = crypto.createHash('sha1').update(cid).digest('hex')
         p = p.replace(new RegExp(cid, "g"), hcid)
      }
   }

   return p
}

var hash = function(p){

   p = _hash(p, /c_[a-z0-9]{32}/)
   p = _hash(p, /[a-z0-9]{32}/)
   p = _hash(p, /a[a-f,0-9]{7}-[a-f,0-9]{4}-4[a-f,0-9]{3}-[a-f,0-9]{4}-[a-f,0-9]{12}/)
   p = _hash(p, /0[a-f,0-9]{7}-[a-f,0-9]{4}-4[a-f,0-9]{3}-[a-f,0-9]{4}-[a-f,0-9]{12}/ )
   p = _hash(p, /s_[a-z0-9]{32}-[0-9]{1,10}/)
   p = _hash(p, /t_[a-z0-9]{32}-[0-9]{1,10}/)

   return p

}


var logIncoming = function(logger, msg){

   for (var i in msg.dao_actions){
      var act = msg.dao_actions[i]

      var info = {
         title       : "dao_action",
         uuid        : msg.clients[0].uuid,
         connId      : msg.clients[0].connId,
         mong2       : msg.mongrel_sink.spec,
         action      : act.action,
         bucket      : act.bucket,
         resp_type   : act.resp_type,
         keys        : Object.keys(act)
      }

      //UPDATE_PERMISSIONS
      //GENERIC_READ
      //GENERIC_CREATE
      //GENERIC_DELETE

      if ("GET" === act.action){
         info.database    = hash(act.database)
         info.resolve     = act.resolve
         info.third_party_cloudlet = hash(act.third_party_cloudlet)
         info.api_key     = hash(act.api_key)
         info.third_party = hash(act.third_party)
         //info.object_data = (undefined !== act.object_data) ? Object.keys(act.object_data) : "-"
      }

      if ("POST" === act.action || "PUT" === act.action ){
         info.database    = hash(act.database)
         info.name        = hash(act.name)
         info.data        = Object.keys(act.object_data)
         info.api_key     = hash(act.api_key)
         info.cloudlet_id = hash(act.cloudlet_id)
         info.id          = hash(act.id)
         info.third_party = hash(act.third_party)
      }

      if ("VIEW" === act.action){
         info.design_doc = act.design_doc
         info.view_name  = act.view_name
         info.limit      = act.meta.limit
         info.offset     = act.meta.offset
         info.id_only    = act.id_only
      }

      //logger.info(Object.keys(msg))
      logger.info(info)

      //logger.info(act)
   }



}


var dao = function(config) {

   var logger = loglet.createLogger(config.logging.name, config.logging.log_level, config.logging.log_file_name)

   helper.init(logger, config.tracklet_worker, config.perms_propagator_f, config.couchbase_cluster, config.couchbase_n1ql);
   subscriptionHandler.init((config));

   zmqM2Node.receiver(config.dao_sink, null, function(msg) {

      logIncoming(logger, msg)

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