'use strict';
const Promise = require('pinkie-promise');
const PouchDB = require('pouchdb');
const pad = require('pad-number');
const random = require("random-js")();

const couchdbUrl = 'https://dev.wxsweb.com:6984/';
const couchdbAuth = {
  username: 'wxs.readonly@wxsweb.com',
  password: 'wxs.readonly@wxsweb.com'
};

const contactBankDbPattern = 'contactbankdb-store{STORE_ID}';
const contactBankCafeDbPattern = 'contactbankcafedb-store{STORE_ID}';
const contactBankPOSDbPattern = 'contactbankposdb-store{STORE_ID}';
const batchSize = 1000;

/**
 * A function that is called before document is pushed to server.
 * Can be used to modify document before storing.
 * 
 * @param {Object} doc - A couchdb document
 * @returns {Object} Modified document
 */
function updateDoc(doc){
  //// CODE BLOCK START: To make string of length 10 by padding 0 as prefix
  //doc.id = parseInt(doc._id);
  //doc._id = pad(doc._id, 10);
  //// CODE BLOCK END: To make string of length 10 by padding 0 as prefix
  
  //// CODE BLOCK START: To create id by (doc.transactionTimestamp + RANDOM_NUM)
  var timeStamp = doc.transactionTimestamp || new Date().getTime();
  var randomNumber = pad(random.integer(1, 99999), 6);
  doc.id = parseInt(timeStamp + randomNumber)
  doc._id = timeStamp + '_' + randomNumber;
  //// CODE BLOCK END: To create id by (doc.transactionTimestamp + RANDOM_NUM)
  return doc;
}

/**
 * Identify type of document.
 * Based on return value, the document will be stored or ignored.
 * 
 * When returns "CAFE", doc is stored on "contactbankcafedb-store-XX"
 * When returns "POS", doc is stored on "contactbankposdb-store-XX"
 * Otherwise, doc is ignored
 * 
 * @param {Object} doc - A couchdb document
 * @returns {String}
 */
function getType(doc){
  if(doc.type == 10){
    return 'CAFE';
  }
  
  if(doc.type){
    return 'POS';
  }
}

function createRemoteDb(dbName, skipSetup){
  return new PouchDB(couchdbUrl + dbName, {
    auth: couchdbAuth,
    skip_setup: skipSetup
  });
}

function destryAndCreateSplittedDb(storeId, splittedDbs){
  var databases = {
      contactBankCafe: createRemoteDb(contactBankCafeDbPattern.replace('{STORE_ID}', storeId), true),
      contactBankPOS: createRemoteDb(contactBankPOSDbPattern.replace('{STORE_ID}', storeId), true)
  };
  
  return Promise.all([databases.contactBankCafe.destroy(), databases.contactBankPOS.destroy()]).then(function(){
    splittedDbs.contactBankCafe = createRemoteDb(contactBankCafeDbPattern.replace('{STORE_ID}', storeId), false);
    splittedDbs.contactBankPOS = createRemoteDb(contactBankPOSDbPattern.replace('{STORE_ID}', storeId), false);
  });
}

function pushDocs(database, docs){
  //When no document to push in current batch
  if(!docs.length){
    return Promise.resolve();
  }
  //Pushes documents to new database
  return database.bulkDocs(docs);
}

function splitDb(storeId, rows, splittedDbs){
  var promise;
  
  // When copying for first batch, destroy databases if already exists
  if(Object.keys(splittedDbs).length === 0){
    promise = destryAndCreateSplittedDb(storeId, splittedDbs);
  } else {
    promise = Promise.resolve();
  }
  
  return promise.then(function(){
    var contactBankCafeDocs = [];
    var contactBankPOSDocs = [];
    
    rows.forEach(function(row){
      var doc = row.doc;
     
      if(!doc){
        return;
      }
      var type = getType(doc);
      
      //Splitting based on type
      if(type === 'CAFE'){
        doc = updateDoc(doc);
        delete doc._rev; //_rev must be removed, otherwise a conflict error will occur
        contactBankCafeDocs.push(doc);
      } else if(type === 'POS'){
        doc = updateDoc(doc);
        delete doc._rev;
        contactBankPOSDocs.push(doc);
      }
    });
    return Promise.all([pushDocs(splittedDbs.contactBankCafe, contactBankCafeDocs), pushDocs(splittedDbs.contactBankPOS, contactBankPOSDocs)]);
  });
}

function fetchAndSplitDb(storeId, contactBankDb, startkey, splittedDbs){
  return contactBankDb.allDocs({
    include_docs: true,
    limit: batchSize,
    startkey: startkey,
    skip: startkey ? 1 : 0
  }).then(function(result){
    return splitDb(storeId, result.rows, splittedDbs).then(function(){
      if((result.offset + result.rows.length) < result.total_rows){
        //Invoke for next batch
        return fetchAndSplitDb(storeId, contactBankDb, result.rows[result.rows.length - 1].key, splittedDbs);
      }
    });
  });
}

function convertToSeconds(ms){
  return (ms / 1000).toFixed(3);
}

function splitAndCopyDb(storeId){
  
  if(!storeId){
    return Promise.reject('Argument "storeId" is missing for function "splitAndCopyDb"');
  }
  
  var start = new Date().getTime();
  var contactBankDbName = contactBankDbPattern.replace('{STORE_ID}', storeId);
  var contactBankDb = createRemoteDb(contactBankDbName, true);
  
  console.log('Started splitting of database: "' + contactBankDbName + '"...');
  
  return fetchAndSplitDb(storeId, contactBankDb, null, {}).then(function(){
    var end = new Date().getTime();
    console.log('Finished splitting of database: "' + contactBankDbName + '" after ' + convertToSeconds(end - start) + ' seconds');
  }).catch(function(e){
    throw new Error(JSON.stringify(e));
  });
}

module.exports = splitAndCopyDb;