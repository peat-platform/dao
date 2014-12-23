'use strict';

var Lazy       = require('lazy.js');
var async      = require('async');
var HTTPStatus = require('http-status');
var couchbase  = require('couchbase');
var uuid       = require('uuid');

var dbc         = require('dbc');
var openiLogger = require('openi-logger');
var zmq         = require('m2nodehandler');
var openiUtils  = require('openi-cloudlet-utils');
var conf        = require('./config.js');
var loglet      = require('loglet');
var Dropbox     = require("dropbox");

var permissionsHelper = require('./permissionsHelper')

var logger   = null;
var cluster  = null;
var dbs      = {};

loglet = loglet.child({component: 'dao'});


var init = function (logger_params) {

   if(logger == undefined)
      logger = openiLogger(logger_params);

   if(cluster == undefined)
      cluster = new couchbase.Cluster( 'couchbase://localhost' );

   dbs['objects']     = cluster.openBucket('objects');
   dbs['types']       = cluster.openBucket('types');
   dbs['attachments'] = cluster.openBucket('attachments');
   dbs['permissions'] = cluster.openBucket('permissions');
};


var getCouchbaseError = function (err, documentName, id) {

   if(err) {
      loglet.error(err);
   }

   switch (err.code) {
      case 4:
         return 'Object too big';
      case 13:
         return 'Entity with that id does not exist';
      case 12:
         if (0 === documentName.indexOf('t_')) {
            return 'OPENi Type already exists (' + id + ').';
         }
         return 'Incorrect revision number provided';
      default:
         return "Reason unknown" + err.code;
   }
};


var getDb = function (name) {
   switch (name) {
      case "dropbox":

         return {
            add : function(database, data, options, callback){

               var dropBoxClient = new Dropbox.Client({
                  key     : "hfgca3g0ty9k5il",
                  secret  : "6c71h4poaqak5zs"
               });

               dropBoxClient.setCredentials({"token" : options.dropbox_token})

               dropBoxClient.writeFile(database, JSON.stringify(data), function(error, stat) {
                  callback(error, stat)
               });
            },
            get : function(database, options, callback){

               var dropBoxClient = new Dropbox.Client({
                  key     : "hfgca3g0ty9k5il",
                  secret  : "6c71h4poaqak5zs"
               });

               dropBoxClient.setCredentials({"token" : options.dropbox_token})

               dropBoxClient.readFile(database, function(error, obj){
                  callback(error, {'value' : JSON.parse(obj) })
               })
            }
         }
   case "attachments":
      return dbs['attachments'];
   case "types":
      return dbs['types'];
   case "permissions":
      return dbs['permissions'];
   case "objects":
         return dbs['objects'];
   }
};

var getDbLazy = function (name, cb) {

   if(logger == undefined)
      logger = openiLogger(logger_params);

   if(cluster == undefined)
      cluster = new couchbase.Cluster( 'couchbase://localhost' );

   if(dbs[name] == undefined);
      dbs[name] = cluster.openBucket(name, cb); 

   return dbs[name];
};

var genericHasAccess = function (msg, callback, cb) {

   var db_use = getDbLazy('dbkeys', function(err)
   {
      var key = '';
      if(msg.authorization)
         key = msg.authorization.replace("Bearer ", "");
      else
      {
         callback(null, { 'error': 'Access to db denied: No key, no service' }, HTTPStatus.NOT_FOUND);
         return;
      }

      if(err)
      {
         callback(null, { 'error': 'Error adding entity: ' + getCouchbaseError(err, msg.database, msg.id) }, HTTPStatus.INTERNAL_SERVER_ERROR);
         return;
      }

      db_use.get(msg.database, function (err, result) {
         if (err || result.value.key !== key) {
            db_use.get(key, function (err, result) {
               if (err || Array.isArray(result.value.dbs) && result.value.dbs.indexOf(key) > -1) {
                  //loglet.error('Access to db denied: DB does not exist or key wrong');
                  callback(null, { 'error': 'Access to db denied: DB does not exist or key wrong' }, HTTPStatus.NOT_FOUND);
                  //cb(false);
               }
               else
                  cb(true);
            });
         }
         else
            cb(true);
      });
   });
}

var genericCreate = function (msg, callback) {

   var db_use = getDbLazy(msg.database, function(err)
   {
      if(err)
      {
         callback(null, { 'error': 'Error adding entity: ' + getCouchbaseError(err, msg.database, msg.id) }, HTTPStatus.INTERNAL_SERVER_ERROR);
         return;
      }

      if(msg.id == undefined) /*fallback to randfom id*/
         msg.id = msg.database + '_' + uuid.v4();;

      db_use.insert(msg.id, msg.data, msg.options, function (err, result) {
         if (err) {
            callback(null, { 'error': 'Error adding entity: ' + getCouchbaseError(err, msg.database, msg.id) }, (12 === err.code) ? HTTPStatus.CONFLICT : HTTPStatus.BAD_REQUEST);
         }
         else {
            callback(null, { 'request': msg, 'data': msg.data }, HTTPStatus.CREATED);
         }
      });

   });
};

var genericRead = function (msg, callback) {

   var db_use = getDbLazy(msg.database, function(err)
   {
      if(err)
      {
         callback(null, { 'error': 'Error reading entity: ' + getCouchbaseError(err, msg.database, msg.id) }, HTTPStatus.INTERNAL_SERVER_ERROR);
         return;
      }

      if(msg.id == undefined)
      {
         callback(null, { 'error': 'Error no entity ID specified: ' + getCouchbaseError(err, msg.database, msg.id) }, HTTPStatus.BAD_REQUEST);
         return;
      }

      db_use.get(msg.id, msg.options, function (err, result) {
         if (err) {
            callback(null, { 'error': 'Error reading entity: ' + getCouchbaseError(err, msg.database, msg.id) }, HTTPStatus.NOT_FOUND);
         }
         else {
            callback(null, { 'request': msg, 'data': result.value }, HTTPStatus.OK);
         }
      });

   });
};

var genericUpdate = function (msg, callback) {

   var db_use = getDbLazy(msg.database, function(err)
   {
      if(err)
      {
         callback(null, { 'error': 'Error update entity: ' + getCouchbaseError(err, msg.database, msg.id) }, HTTPStatus.INTERNAL_SERVER_ERROR);
         return;
      }

      if(msg.id == undefined)
      {
         callback(null, { 'error': 'Error no entity ID specified: ' + getCouchbaseError(err, msg.database, msg.id) }, HTTPStatus.BAD_REQUEST);
         return;
      }

      db_use.get(msg.id, msg.options, function (err, result_old) {
         if (err) {
               callback(null, { 'error': 'Error update entity: ' + getCouchbaseError(err, msg.database, msg.id) }, HTTPStatus.NOT_FOUND);
               return;
         }

         db_use.upsert(msg.id, msg.data, msg.options, function (err, result) {
            if (err) {
               callback(null, { 'error': 'Error update entity: ' + getCouchbaseError(err, msg.database, msg.id) }, HTTPStatus.BAD_REQUEST);
            }
            else {
               callback(null, { 'request': msg, 'data': result_old.value }, HTTPStatus.OK);
            }
         });
      });

   });
};

var genericDelete = function (msg, callback) {

   var db_use = getDbLazy(msg.database, function(err)
   {
      if(err)
      {
         callback(null, { 'error': 'Error reading entity: ' + getCouchbaseError(err, msg.database, msg.id) }, HTTPStatus.INTERNAL_SERVER_ERROR);
         return;
      }

      if(msg.id == undefined)
      {
         callback(null, { 'error': 'Error no entity ID specified: ' + getCouchbaseError(err, msg.database, msg.id) }, HTTPStatus.BAD_REQUEST);
         return;
      }

      db_use.get(msg.id, msg.options, function (err, result2) {
         if (err) {
            callback(null, { 'error': 'Error update entity: ' + getCouchbaseError(err, msg.database, msg.id) }, HTTPStatus.NOT_FOUND);
            return;
         }
         db_use.remove(msg.id, msg.options, function (err, result) {
            if (err) {
               callback(null, { 'error': 'Error reading entity: ' + getCouchbaseError(err, msg.database, msg.id) }, HTTPStatus.NOT_FOUND);
            }
            else {
               callback(null, { 'request': msg, 'data': result2.value }, HTTPStatus.OK);
            }
         });
      });

   });
};

function merger(a, b) { /*old merger, may need revamping on arrays, allow dynamic?*/
   var r;
   if (Array.isArray(a) && Array.isArray(b)) {
      return a.concat(b);
   } else if (Array.isArray(a)) {
      r = [];
      r = a.concat([]);
      r.push(b);
      return r;
   } else if (Array.isArray(b)) {
      r = [];
      r = b.concat([]);
      r.push(a);
      return r;
   } else if (typeof a === 'object' && typeof b === 'object') {
      r = {};
      var ps = Lazy(Object.getOwnPropertyNames(a)).concat(Object.getOwnPropertyNames(b)).uniq().toArray();
      for(var p in ps) {
         if(ps.hasOwnProperty(p)) {
            r[ps[p]] = merger(a[ps[p]], b[ps[p]]);
         }
      }
      return r;
   } else if (typeof a === typeof b) {
      return b;
   } else if (typeof a === 'undefined') {
      return b;
   }
   //else if(typeof b === 'undefined')
   // return a;
   return a;
}

var genericPatch = function (msg, callback) {

   var db_use = getDbLazy(msg.database, function(err)
   {
      if(err)
      {
         callback(null, { 'error': 'Error update entity: ' + getCouchbaseError(err, msg.database, msg.id) }, HTTPStatus.INTERNAL_SERVER_ERROR);
         return;
      }

      if(msg.id == undefined)
      {
         callback(null, { 'error': 'Error no entity ID specified: ' + getCouchbaseError(err, msg.database, msg.id) }, HTTPStatus.BAD_REQUEST);
         return;
      }

      db_use.get(msg.id, msg.options, function (err, result) {
         if (err) {
               callback(null, { 'error': 'Error update entity: ' + getCouchbaseError(err, msg.database, msg.id) }, httpStatus.NOT_FOUND);
               return;
         }

         var o = Lazy(msg.data).merge(result.value, merger).toObject();

         db_use.upsert(msg.id, o, msg.options, function (err, result) {
            if (err) {
               callback(null, { 'error': 'Error update entity: ' + getCouchbaseError(err, msg.database, msg.id) }, httpStatus.BAD_REQUEST);
            }
            else {
               callback(null, { 'request': msg, 'data': o }, HTTPStatus.OK);
            }
         });
      });

   });
};

var checkPermissions = function(msg, callback, success_function){

   if (openiUtils.isTypeId(msg.database) || openiUtils.isAttachmentId(openiUtils.extractAttachmentId(msg.database))){
      success_function()
   }
   else{

      var cloudlet_id = msg.cloudlet_id
      var third_party = msg.third_party

      var key         = third_party + "+" + cloudlet_id

      dbs['permissions'].get(key, function (err, db_body) {

         if(err) {
            loglet.error(err);
            callback(null, { 'error': 'permission denied' }, HTTPStatus.UNAUTHORIZED);
         }
         else{
            var type_id = msg.object_data['@openi_type']

            if (undefined === db_body.value._current.perms['@types'][type_id]){
               callback(null, { 'error': 'permission denied' }, HTTPStatus.UNAUTHORIZED);
               return
            }
            var create_a = db_body.value._current.perms['@types'][type_id]['@app_level'].create
            var create_b = db_body.value._current.perms['@types'][type_id]['@cloudlet_level'].create
            var read_a   = db_body.value._current.perms['@types'][type_id]['@app_level'].read
            var read_b   = db_body.value._current.perms['@types'][type_id]['@cloudlet_level'].read
            var update_a = db_body.value._current.perms['@types'][type_id]['@app_level'].update
            var update_b = db_body.value._current.perms['@types'][type_id]['@cloudlet_level'].update
            var delete_a = db_body.value._current.perms['@types'][type_id]['@app_level'].delete
            var delete_b = db_body.value._current.perms['@types'][type_id]['@cloudlet_level'].delete

            var create = (create_a || create_b)
            var read   = (read_a   || read_b  )
            var update = (update_a || update_b)
            var delet  = (delete_a || delete_b)

            if (create){
               var permissions = { 'created_by' : third_party }

               permissions[ third_party ] = {
                  "create" : create,
                  "read"   : read,
                  "update" : update,
                  "delete" : delet
               }

               success_function(permissions)
            }
            else{
               callback(null, { 'error': 'permission denied' }, HTTPStatus.UNAUTHORIZED);
            }
         }
      });
   }
}

var postAction = function (msg, callback) {

   var options = (undefined === msg.format) ? {} : {format: msg.format };

   if (undefined !== msg.dropbox_token){
      options.dropbox_token = msg.dropbox_token
   }
   var db_use = getDb(msg['bucket']);

   checkPermissions(msg, callback, function(permission){

      msg.object_data._permissions = permission

      db_use.insert(msg.database, msg.object_data, options, function (err, result) {
         if (err) {
            var httpCode = (12 === err.code) ? HTTPStatus.CONFLICT : HTTPStatus.INTERNAL_SERVER_ERROR;

            if(err.code === 12 && 0 === openiUtils.isTypeId(msg.database))
            {
               callback(null, { '@id': msg.id }, HTTPStatus.OK);
            }
            else {
               callback(null, { 'error': 'Error adding entity: ' + getCouchbaseError(err, msg.database, msg.id) }, httpCode);
            }
         }
         else {
            callback(null, { '@id': msg.id }, HTTPStatus.CREATED);
         }
      });
   })


};


var putAction = function (msg, callback) {

   var db_use = getDb(msg['bucket']);

   if(msg.revision === undefined){
      callback(null, { 'error': '_revision attribute missing'}, HTTPStatus.BAD_REQUEST);
   }

   db_use.get(msg.database, function (err, db_body) {

      if(err) {
         loglet.error(err);
      }

      var revisionParts = msg.revision.split('-');

      if (db_body.cas['0'].toString() !== revisionParts[0] && db_body.cas['1'].toString() !== revisionParts[1]) {
         callback(null, { 'error': 'Entity already updated'}, HTTPStatus.CONFLICT);
         return;
      }

      if (db_body.value['@openi_type'] === msg.object_data['@openi_type'] && db_body.cas['1'].toString() !== revisionParts[1]) {
         callback(null, { 'error': 'Entity already updated'}, HTTPStatus.CONFLICT);
         return;
      }

      if ( undefined !== db_body.value._permissions[msg.third_party] || db_body.value._permissions[msg.third_party].update ){

         if(db_body.value['_date_created'] !== undefined) {
            msg.object_data['_date_created'] = db_body.value['_date_created'];
         }

         var options = (undefined === msg.format) ? {} : {format: msg.format };

         db_use.replace(msg.database, msg.object_data, options, function (err, result) {

            if (err) {
               logger.log('error', err);
               loglet.error(err);

               var httpCode = (12 === err.code) ? HTTPStatus.CONFLICT : HTTPStatus.INTERNAL_SERVER_ERROR;

               callback(null, { 'error': 'Error updating entity: ' + getCouchbaseError(err) }, httpCode);
            }
            else {
               callback(null, { '@id': msg.id }, HTTPStatus.OK);
            }
         });

      }
      else{
         callback(null, { 'error': 'Permission denied' }, HTTPStatus.UNAUTHORIZED);
      }

   });
};


var recurseReplaceSubRef = function (sub, subs) {
   var subrep;
   var o;
   //first two switch statements are for results with just property values, second are for full objects
   if (openiUtils.isObjectId(sub)){
      for (var j in subs) {
         if (subs.hasOwnProperty(j)) {
            subrep = subs[j];
            if (subrep.id === sub) {
               o = recurseReplaceSubRef(subrep.value, subs);
               sub = o['@data'];
            }
         }
      }
   }
   else if (openiUtils.isAttachmentId(sub)){
      for (var j in subs) {
         if (subs.hasOwnProperty(j)) {
            subrep = subs[j];
            if (subrep.id === sub) {
               o = recurseReplaceSubRef(subrep.value, subs);
               sub = o;
            }
         }
      }
   }

   var data = (undefined === sub['@data']) ? sub : sub['@data'];

   for (var i in data) {
      if (data.hasOwnProperty(i)) {
         var val = data[i];

         if (i === 'id' || i === '@id'){
            continue;
         }

         if (typeof val === 'string' && openiUtils.isObjectId(val)) {

            for (var j in subs) {
               if (subs.hasOwnProperty(j)) {
                  subrep = subs[j];
                  if (subrep.id === val) {
                     o = recurseReplaceSubRef(subrep.value, subs);
                     data[i] = o['@data'];
                  }
               }
            }
         }
         else if (Array.isArray(val)){
            for ( var k = 0; k < val.length; k++){
               for (var j in subs) {
                  if (subs.hasOwnProperty(j)) {
                     subrep = subs[j];
                     for (var u = 0; u < subrep.id.length; u++){
                        if (val[k] === subrep.id[u]) {
                           if (undefined !== subrep.value[u]){
                              o = recurseReplaceSubRef(subrep.value[u], subs);
                              data[i][k] = o['@data'];
                           }
                        }
                     }
                  }
               }
            }
         }
         else if (typeof val === 'string' &&  openiUtils.isAttachmentId(val) ) {
            for (var j in subs) {
               if (subs.hasOwnProperty(j)) {
                  subrep = subs[j];
                  if (subrep.id === val) {
                     o = recurseReplaceSubRef(subrep.value, subs);
                     data[i] = o;
                  }
               }
            }
         }
      }
   }

   return sub;
};


var getAction = function (msg, callback) {

   var options = {}

   if (undefined !== msg.dropbox_token){
      options.dropbox_token = msg.dropbox_token
   }

   var db_use = getDb(msg['bucket']);

   db_use.get(msg.database, options, function (err, db_body) {

      if (err) {
         logger.log('error', err);
         callback(null, { 'error': 'Not Found' }, HTTPStatus.NOT_FOUND);
         loglet.error(err);
      }
      else {
         if (openiUtils.isTypeId(msg.database) || openiUtils.isCloudletId(msg.database) || -1 !== ["permissions", "binary", "binary_meta", "attachments"].indexOf( msg.resp_type ) ){
            processGetData(db_body, msg, callback);
         }
         else if ( undefined === db_body.value._permissions ||
                     undefined === db_body.value._permissions[msg.third_party] ||
                     !db_body.value._permissions[msg.third_party].read ){
            callback(null, { 'error': 'Permission denied' }, HTTPStatus.UNAUTHORIZED);
         }
         else{
            processGetData(db_body, msg, callback);
         }
      }
   });
};




var processArrObjects = function (arr, msg, callback) {

   var keys = [];
   var resp = {
      "meta": msg.meta,
      result: []
   };

   for ( var i = 0; i < arr.length; i++){
      keys.push(arr[i].id);
   }

   if (0 === keys.length){
      resp.meta.next = null;
      callback(null, resp, HTTPStatus.OK);
      return;
   }


   getDb(msg['bucket']).getMulti(keys, function (err, db_body) {

      if (err) {
         logger.log('error', err);
         loglet.error(err);
         callback(null, { 'error': 'Error getting data from datastore' }, HTTPStatus.INTERNAL_SERVER_ERROR);
      }
      else {

         for (var i in db_body){
            if (msg.resp_type === 'type'){
               resp.result.push(openiUtils.typeHelper(db_body[i], msg));
            }
            else if (msg.resp_type === 'cloudlet') {
               resp.result.push(openiUtils.cloudletHelper(db_body[i], msg));
            }
            else{
               if ( undefined === db_body[i].value._permissions[msg.third_party] || db_body[i].value._permissions[msg.third_party].read ){
                  var obj = openiUtils.objectHelper(db_body[i], msg)
                  delete obj._permissions
                  delete obj._revision
                  resp.result.push(obj);
               }
            }
         }

         resp.meta.total_count = resp.result.length;

         if (resp.meta.limit > resp.result.length){
            resp.meta.next = null;
         }

         callback(null, resp, HTTPStatus.OK);
      }
   });
};

var getRespType = function(obj, msg){

   var resp_type = 'object';

   if (openiUtils.isAttachmentId(obj.id)){
      resp_type = (msg.meta === 'true') ? 'attachment' : 'binary';

      if ( 0 !== obj.key){
         resp_type = 'attachment';
      }
   }

   return resp_type;
};

var getObj = function(db_body, msg){

   if ( undefined === db_body.value._permissions[msg.third_party] || !db_body.value._permissions[msg.third_party].read ){
      return null
   }

   var obj = {};

   if (msg['resp_type'] === "attachment"){
      db_body.value['file'] = undefined;
      obj                   = db_body.value;
   }
   else{
      obj = openiUtils.objectHelper(db_body, msg);
   }

   obj._permissions = undefined

   if(undefined !== msg.property && "" !== msg.property){
      var parts = msg.property.split('.');
      for (var i = 0; i < parts.length; i++){
         obj = obj[parts[i]];
      }
   }

   return obj;
};


//Takes in database response, iterates over the objects body looking for references to sub objects,
//creates an array of sub objects and RECURSIVLY requests subobjects until all are stored on the message.subids
//array. The original object is then populated
var processSubObjects = function (db_body, msg, callback) {

   //overused, requires refactor
   var obj = getObj(db_body, msg);

   var i;
   var sub;


   if (undefined === msg.resolve || msg.resolve === false) {
      //forward message to get with current object and say replace..... recursive call.... dangerous!!!
      //loop through find object references;
      callback(null, obj, HTTPStatus.OK, zmq.standard_headers.json);
   }
   else {
      var new_msg         = {};
      new_msg.subIds      = (undefined === msg.subIds     ) ? [] : msg.subIds;
      new_msg.originalObj = (undefined === msg.originalObj) ? obj : msg.originalObj;
      new_msg.resolve     = true;

      if ( openiUtils.isObjectId(obj) || openiUtils.isAttachmentId(obj) ) {
         new_msg.subIds.push({'key': 0, 'id': obj});
      }

      var data = (undefined === obj['@data']) ? obj : obj['@data'];

      for (i in data) {
         if (data.hasOwnProperty(i)) {
            var val = data[i];
            if ( openiUtils.isObjectId(val) || openiUtils.isAttachmentId(val) ) {
               if ('id' !== i || '@id' !== i){
                  new_msg.subIds.push({'key': i, 'id': val});
               }
            }
            else if (Array.isArray(val)){
               var tmp = []
               for (var j = 0; j < val.length; j++){
                  if ( openiUtils.isObjectId(val[j]) || openiUtils.isAttachmentId(val[j]) ) {
                     tmp.push(val[j]);
                  }
               }
               if (tmp.length > 0){
                  new_msg.subIds.push({'key': i, 'id': tmp});
               }
            }
         }
      }


      for (i in new_msg.subIds) {
         if (new_msg.subIds.hasOwnProperty(i)) {
            sub = new_msg.subIds[i];
            if (Array.isArray(sub.id)){
               for ( var j = 0; j < sub.id.length; j++){
                  if (sub.id[j] === obj['@id']) {
                     if (!Array.isArray(new_msg.subIds[i].value)){
                        new_msg.subIds[i].value = []
                     }
                     new_msg.subIds[i].value[j] = obj;
                  }
               }
            }
            else if (sub.id === obj['@id'] || sub.id === obj['id']) {
               new_msg.subIds[i].value = obj;
            }
         }
      }


      for (i in new_msg.subIds) {
         if (new_msg.subIds.hasOwnProperty(i)) {
            sub = new_msg.subIds[i];

            if (Array.isArray(sub.id)){
               for (var j = 0; j < sub.id.length; j++){
                  if (undefined === sub.value || undefined === sub.value[j]){
                     var id              = sub.id[j]
                     var cloudletId      = msg.database.split('+')[0];
                     new_msg.action      = 'GET';
                     new_msg.database    = cloudletId + '+' + id;
                     new_msg.bucket      = (openiUtils.isObjectId(id)) ? 'objects'  : 'attachments';
                     new_msg.resp_type   = getRespType(sub, msg);
                     new_msg.third_party = msg.third_party

                     getAction(new_msg, callback);
                     return;
                  }
               }
            }
            else if (undefined === sub.value) {
               var cloudletId      = msg.database.split('+')[0];
               new_msg.action      = 'GET';
               new_msg.database    = cloudletId + '+' + sub.id;
               new_msg.bucket      = (openiUtils.isObjectId(sub.id)) ? 'objects'  : 'attachments';
               new_msg.resp_type   = getRespType(sub, msg);
               new_msg.third_party = msg.third_party

               getAction(new_msg, callback);
               return;
            }
         }
      }

      var oobj = recurseReplaceSubRef(new_msg.originalObj, new_msg.subIds);

      callback(null, oobj, HTTPStatus.OK, zmq.standard_headers.json);
   }
};

var permissionsObj = function(db_body, msg){

   var perms   = []

   var objects = db_body.value._current.perms["@objects"]
   var types   = db_body.value._current.perms["@types"]


   for ( var obj in objects ){
      if (objects[obj].CREATE){
         perms.push({'ref':obj, "type": 'object', 'access_level': 'CLOUDLET', 'access_type': 'CREATE'})
      }
      if (objects[obj].READ){
         perms.push({'ref':obj, "type": 'object', 'access_level': 'CLOUDLET', 'access_type': 'READ'})
      }
      if (objects[obj].UPDATE){
         perms.push({'ref':obj, "type": 'object', 'access_level': 'CLOUDLET', 'access_type': 'UPDATE'})
      }
      if (objects[obj].DELETE){
         perms.push({'ref':obj, "type": 'object', 'access_level': 'CLOUDLET', 'access_type': 'DELETE'})
      }
   }


   for (var type in types){
      if (undefined !== types[type]['@app_level']){
         if (types[type]['@app_level'].CREATE){
            perms.push({'ref':type, "type": 'type', 'access_level': 'APP', 'access_type': 'UPDATE'})
         }
         if (types[type]['@app_level'].READ){
            perms.push({'ref':type, "type": 'type', 'access_level': 'APP', 'access_type': 'READ'})
         }
         if (types[type]['@app_level'].UPDATE){
            perms.push({'ref':type, "type": 'type', 'access_level': 'APP', 'access_type': 'UPDATE'})
         }
         if (types[type]['@app_level'].DELETE){
            perms.push({'ref':type, "type": 'type', 'access_level': 'APP', 'access_type': 'DELETE'})
         }
      }
      if (undefined !== types[type]['@cloudlet_level']){
         if (types[type]['@cloudlet_level'].CREATE){
            perms.push({'ref':type, "type": 'type', 'access_level': 'CLOUDLET', 'access_type': 'UPDATE'})
         }
         if (types[type]['@cloudlet_level'].READ){
            perms.push({'ref':type, "type": 'type', 'access_level': 'CLOUDLET', 'access_type': 'READ'})
         }
         if (types[type]['@cloudlet_level'].UPDATE){
            perms.push({'ref':type, "type": 'type', 'access_level': 'CLOUDLET', 'access_type': 'UPDATE'})
         }
         if (types[type]['@cloudlet_level'].DELETE){
            perms.push({'ref':type, "type": 'type', 'access_level': 'CLOUDLET', 'access_type': 'DELETE'})
         }
      }
   }

   return perms
}


var processGetData = function (db_body, msg, callback) {

   var headers = zmq.standard_headers.json;
   var resp    = null;

   switch (msg['resp_type']) {
      case 'cloudlet':
         resp = openiUtils.cloudletHelper(db_body, msg);
         break;
      case 'binary':
         headers = {'Content-Type': db_body.value["Content-Type"]};
         resp = new Buffer(db_body.value['file'], 'base64');
         break;
      case 'binary_meta':
         db_body.value['file'] = undefined;
         resp = db_body.value;
         break;
      case 'type':
         resp = openiUtils.typeHelper(db_body, msg);
         break;
      case 'permissions':
         resp = permissionsObj(db_body, msg);
         break;
      case 'object':
      default:
         processSubObjects(db_body, msg, callback);
         break;
   }

   if (null !== resp) {
      callback(null, resp, HTTPStatus.OK, headers);
   }
};


//var getOrPostAction = function (msg, callback) {
//
//   var db_use = getDb(msg['bucket']);
//
//   db_use.get(msg.database, function (err, db_body) {
//
//      if (err) {
//         postAction(msg, callback);
//         loglet.error(err);
//      }
//      else {
//         processGetData(db_body, msg, callback);
//      }
//   });
//};


var viewAction = function (msg, callback) {

   msg.meta          = (undefined === msg.meta) ? {} : msg.meta;
   msg.meta.limit    = (undefined === msg.meta || typeof msg.meta.limit  !== 'number' || isNaN(msg.meta.limit ) || msg.meta.limit  > 30) ? 30 : msg.meta.limit ;
   msg.meta.offset   = (undefined === msg.meta || typeof msg.meta.offset !== 'number'   || isNaN(msg.meta.offset)) ? 0 : msg.meta.offset;
   msg.startkey      = typeof  msg.startkey !== 'string' ? "" : msg.startkey;


   var ViewQuery = couchbase.ViewQuery;
   var query = ViewQuery.from(msg.design_doc, msg.view_name)
      .skip(msg.meta.offset)
      .limit(msg.meta.limit)
      .stale(ViewQuery.Update.BEFORE)
      .id_range(msg.startkey, msg.startkey)


   if (undefined !== msg.reduce && msg.reduce ) {
      query.reduce(msg.reduce)
   }
   else {
      query.reduce(false)
   }

   if (undefined !== msg.start_key) {
      if (undefined === msg.end_key){
         msg.end_key = msg.start_key
      }
      query.range(msg.start_key, msg.end_key, true)
   }

//   if (undefined !== msg.group && msg.group) {
//      query.group(msg.group)
//   }

   if (undefined !== msg.group_level ) {
      query.group(msg.group_level)
   }

   delete query.options.group

   getDb(msg['bucket']).query(query, function (err, res) {

      var respObj = {
         "meta": msg.meta,
         result: []
      };

      if (err) {
         callback(null, { 'error': 'Error getting view: ' + getCouchbaseError(err) },
            HTTPStatus.INTERNAL_SERVER_ERROR);
      }
      else if (res !== null) {

         if (msg.resp_type === 'type') {
            if (msg.id_only){
               for (var i = 0; i < res.length; i++) {
                  respObj.result[i] = openiUtils.typeHelper(res[i][2], msg);
               }
            }
            else{
               processArrObjects(res, msg, callback);
               return;
            }
         }
         else if (msg.resp_type === 'type_stats') {
            for (var i = 0; i < res.length; i++) {
               respObj.result[i] = openiUtils.typeStats(res[i], msg);
            }
         }
         else if (msg.resp_type === 'cloudlet') {

            for (var i = 0; i < res.length; i++) {
               respObj.result[i] = res[i].key;
            }
         }
         else {
            if (msg.id_only){
               for (var i = 0; i < res.length; i++) {
                  respObj.result[i] = {'@id' : res[i]['value']};
               }
            }
            else{
               processArrObjects(res, msg, callback);
               return;
            }
         }
      }

      respObj.meta.total_count = respObj.result.length;

      if (respObj.meta.limit > respObj.result.length){
         respObj.meta.next = null;
      }

      callback(null, respObj, HTTPStatus.OK);
   });
};


var fetchAction = function (msg, callback) {

   getDb('openi').get(msg.database, function (err, db_body) {
      if (err) {
         logger.log('error', err);
         loglet.error(err);
         callback(null, { 'error': 'Error getting entity: ' + getCouchbaseError(err) },
            HTTPStatus.INTERNAL_SERVER_ERROR);
      }
      else {
         callback(null, { 'data': db_body }, HTTPStatus.OK);
      }
   });
};


var createDBAction = function (msg, callback) {

   getDb('objects').add(msg.database, {}, function (err, res) {

      if (err && err.code !== 12) {
         //logger.log('error', err)
         loglet.error(err);
         callback(null, { 'error': 'Error creating entity: ' + getCouchbaseError(err) },
            HTTPStatus.INTERNAL_SERVER_ERROR);
      }
      else {
         logger.log('debug', 'Cloudlet created, id: ' + msg.database);
         callback(null, { 'id': msg.database }, HTTPStatus.OK);
      }
   });
};


var deleteDBAction = function (msg, callback) {

   getDb('objects').get(msg.database, function (err, db_body) {

      if (err) {
         logger.log('error', err);
         loglet.error(err);
         callback(null, { 'error': 'Error deleting entity : ' + getCouchbaseError(err) }, HTTPStatus.NOT_FOUND);
      }
      else if ( undefined === db_body.value._permissions[msg.third_party] || !db_body.value._permissions[msg.third_party].delete ){
         callback(null, { 'error': 'Permission denied' }, HTTPStatus.UNAUTHORIZED);
      }
      else{
         getDb('objects').remove(msg.database, function (err) {
            if (err) {
               logger.log('error', err);
               loglet.error(err);
               callback(null, { 'error': 'Error deleting entity : ' + getCouchbaseError(err) }, HTTPStatus.NOT_FOUND);
            }
            else {
               logger.log('debug', 'entity deleted, id: ' + msg.database);
               callback(null, { 'id': msg.id }, HTTPStatus.OK);
            }
         });
      }
   })


};


var patchAttachmentAction = function (msg) {

   var db_use = getDb('objects');

   db_use.get(msg.database, function (err, db_body) {

      if (err) {
         loglet.error(err);
      }

      db_body.value['_date_modified'] = new Date().toJSON();

      var parts = msg.property.split('.');
      var data  = db_body.value['@data'];

      for (var i = 0; i < parts.length - 1; i++){
         data = data[parts[i]];
      }

      data[parts[parts.length-1]] = msg.object_data;

      db_use.replace(msg.database, db_body.value, {}, function (err, result) {

         if (err) {
            logger.log('info', 'error ' + err);
            loglet.error(err);
         }
         else {
            logger.log('info', 'no error ' + result);
         }
      });
   });
};


var patchAttachmentObjectAction = function (msg) {

   var db_use = getDb('objects');

   db_use.get(msg.database, function (err, db_body) {

      if (err && 13 === err.code){
         loglet.error(err);
         db_use.insert(msg.database, msg.object, {}, function (err, result) {
            if (err) {
               logger.log('info', 'error ' + err);
               loglet.error(err);
            }
            else {
               logger.log('info', 'no error ' + result);
            }
         });
      }
      else{
         db_body.value['_date_modified'] = new Date().toJSON();

         db_body.value['@data'][msg.type] = db_body.value['@data'][msg.type].concat(msg.object['@data'][msg.type]);

         db_use.replace(msg.database, db_body.value, {}, function (err, result) {

            if (err) {
               logger.log('info', 'error ' + err);
               loglet.error(err);
            }
            else {
               logger.log('info', 'no error ' + result);
            }
         });
      }
   });
};


var updatePermissions = function (msg, callback) {

   var db_use = getDb('permissions');

   db_use.get(msg.database, function (err, db_body) {

      if (err && 13 === err.code){
         loglet.error(err);
         db_use.insert(msg.database, msg.object, {}, function (err, result) {
            if (err) {
               logger.log('info', 'error ' + err);
               loglet.error(err);
            }
            else {
               logger.log('info', 'no error ' + result);
            }
         });
      }
      else{
         db_body.value['_date_modified'] = new Date().toJSON();

         db_body.value['@data'][msg.type] = db_body.value['@data'][msg.type].concat(msg.object['@data'][msg.type]);

         db_use.replace(msg.database, db_body.value, {}, function (err, result) {

            if (err) {
               logger.log('info', 'error ' + err);
               loglet.error(err);
            }
            else {
               logger.log('info', 'no error ' + result);
            }
         });
      }
   });
};


var evaluateAction = function (msg, callback) {

   dbc.conditionalHasMember(msg, 'database', (msg.action !== 'VIEW' ));
   dbc.hasMemberIn(msg, 'action', ['POST', 'PUT', 'GET', 'FETCH', 'CREATE', 'DELETE', 'VIEW', 'GET_OR_POST', 'PATCH_ATTACHMENT', 'PATCH_ATTACHMENT_OBJECT', 'UPDATE_PERMISSIONS', 'GENERIC_CREATE', 'GENERIC_READ', 'GENERIC_UPDATE', 'GENERIC_DELETE', 'GENERIC_PATCH']);
   dbc.conditionalHasMember(msg, 'object_data', (msg.action === 'POST' || msg.action === 'PUT'));
   dbc.conditionalHasMember(msg, 'revision', (msg.action === 'PUT'));
   
   dbc.conditionalHasMember(msg, 'data', (msg.action === 'GENERIC_CREATE' || msg.action === 'GENERIC_UPDATE' || msg.action === 'GENERIC_PATCH'));
   
   var resp_msg = '';

   switch (msg.action) {
      case 'GENERIC_CREATE':
         genericHasAccess(msg, callback, function () {
            resp_msg = genericCreate(msg, callback);
         });
         break;
      case 'GENERIC_READ':
         genericHasAccess(msg, callback, function () {
            resp_msg = genericRead(msg, callback);
         });
         break;
      case 'GENERIC_UPDATE':
         genericHasAccess(msg, callback, function () {
            resp_msg = genericUpdate(msg, callback);
         });
         break;
      case 'GENERIC_DELETE':
         genericHasAccess(msg, callback, function () {
            resp_msg = genericDelete(msg, callback);
         });
         break;
      case 'GENERIC_PATCH':
         genericHasAccess(msg, callback, function () {
            resp_msg = genericPatch(msg, callback);
         });
         break;
      case 'POST':
         resp_msg = postAction(msg, callback);
         break;
      case 'PUT':
         resp_msg = putAction(msg, callback);
         break;
      case 'GET':
         resp_msg = getAction(msg, callback);
         break;
      case 'GET_OR_POST':
         resp_msg = getOrPostAction(msg, callback);
         break;
      case 'FETCH':
         resp_msg = fetchAction(msg, callback);
         break;
      case 'CREATE':
         resp_msg = createDBAction(msg, callback);
         break;
      case 'DELETE':
         resp_msg = deleteDBAction(msg, callback);
         break;
      case 'VIEW':
         resp_msg = viewAction(msg, callback);
         break;
      case 'PATCH_ATTACHMENT':
         resp_msg = patchAttachmentAction(msg, callback);
         break;
      case 'PATCH_ATTACHMENT_OBJECT':
         resp_msg = patchAttachmentObjectAction(msg, callback);
         break;
      case 'UPDATE_PERMISSIONS':
         resp_msg = permissionsHelper.processPermissionsUpdate(couchbase, dbs, msg, callback)
         break;

   }
   return resp_msg;
};


var actionToFunction = function (action) {
   return (function (action) {
      return function (callback) {
         evaluateAction(action, callback);
      };
   }(action));
};


var evaluateMessage = function (msg, myCallback) {

   dbc.hasMember(msg, 'dao_actions');
   dbc.hasMember(msg, 'clients');

   //dbc.assert(msg.clients.length > 0);
   dbc.assert(msg.dao_actions.length > 0);

   var arr = {};

   for (var i = 0; i < msg.dao_actions.length; i++) {

      var action = msg.dao_actions[i];
      arr[i] = actionToFunction(action);

   }

   async.series(arr, function (err, results, httpStatusCode) {

      if (err) {
         loglet.error(err);
      }
      myCallback(err, results, httpStatusCode);
   });

};


module.exports.init = init;
module.exports.evaluateMessage = evaluateMessage;
