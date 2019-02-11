/* 
   Copyright 2019 Locomote Ltd.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

/* Streaming pipeline functions and utilies. */

const { Pipeline } = require('./pipeline');

const {
    registerHook,
    getCallHooks
} = require('./hooks');

const { lineParser } = require('./parsers');

const {
    lineTransformer,
    jsonlTransformer
} = require('./transformers');

/**
 * Utility function for creating pipelines.
 */
function pipeline( cacheDir ) {
    return new Pipeline( cacheDir );
}

/**
 * Join a stream of JSON lines objects into an array.
 */
async function jsonlStreamJoin( ins ) {
    const result = [];
    await lineParser( ins, line => {
        if( line.length > 0 ) {
            result.push( JSON.parse( line ) );
        }
    });
    return result;
}

module.exports = {
    Pipeline,
    registerHook,
    lineParser,
    lineTransformer,
    jsonlTransformer,
    pipeline,
    jsonlStreamJoin
};
