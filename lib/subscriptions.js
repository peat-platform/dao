/*
 * DAO
 * Subscription.js
 *
 * Copyright (c) 2013 dconway, dmccarthy
 */
'use strict';

var zmq        = require('m2nodehandler');
var peatUtils  = require('cloudlet-utils');

var subpush = null;


/**
 * Subscription Format
 * {
 *    cloudletId : [cloudletID],
 *    objectId   : [objectID],
 *    type       : [Object or Attachment],
 *    method     : [POST, PUT or DELETE]
 */

var init = function (config) {
   subpush = zmq.sender(config.sub_sink);
};

var last = function (arr) {
   return arr[arr.length - 1];
};


var constructSubscriptionMessage = function (cloudletId, objectId, method) {

   var reqType = "";
   if (objectId == null) {
      return;
   }

   if (peatUtils.isAttachmentId(objectId)) {
      reqType = "attachment";
   }
   else {
      reqType = "object";

      var subscription = {
         'cloudletId': cloudletId,
         'objectId'  : objectId,
         'type'      : reqType,
         'method'    : method
      };
      subpush.send(subscription);
   }
};


var checkSubscription = function (msg, httpStatusCode, resp) {

   var action = last(msg.dao_actions);
   if (null != resp && resp["error"] === undefined && (httpStatusCode >= 200 && httpStatusCode <= 299)) {
      if (action.action === "POST" || action.action === "PUT" || action.action === "DELETE") {
         var database = action.database.split("+");
         var cloudlet = database[0];
         var object = database[1];

         constructSubscriptionMessage(cloudlet, object, action.action);
      }
   }

   /*if(resp["error"] === undefined  && (httpStatusCode >= 200 && httpStatusCode <= 299)) {

    switch (action.action) {
    case "PUT":
    constructSubscriptionMessage(cloudlet, object, action.action)
    break;
    case "POST":
    constructSubscriptionMessage(cloudlet, object, action.action)
    break;
    case "DELETE":
    constructSubscriptionMessage(cloudlet, object, action.action)
    break;

    }
    }*/

};


module.exports.init = init;
module.exports.checkSubscription = checkSubscription;