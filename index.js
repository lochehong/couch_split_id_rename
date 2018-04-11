'use strict';
const questions = require('questions');
const commandLineArgs = require('command-line-args');
const splitAndCopyDb = require('./split-and-copy-db');

const options = commandLineArgs([
  { name: 'id', type: String },
  { name: 'help', type: String }
]);

if(options.help !== undefined){
  console.log(' ');
  console.log('Usage: couchdb-util <command>');
  console.log(' ');
  console.log('Options: ');
  console.log('   --id         Store Id (i.e 12)');
  console.log('   --help       Comamnds help');
  return;
}

function askId(){
  questions.askOne({ info:'Enter store id', required: true }, function(id){
    splitAndCopyDb(id);
  });
}

if(options.id !== undefined){
  return splitAndCopyDb(options.id);
} else {
  askId();
}
