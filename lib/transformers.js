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

const { getCallHooks } = require('./hooks');

const { recordParser, lineParser } = require('./parsers');

const NEWLINE = 0x0A;

/**
 * Multi-record input transformer. Takes as input a stream of text data,
 * extracts each line from the input and processes it using the op
 * function, before writing the result to an output stream.
 * @param ins       A readable input stream.
 * @param outs      A writeable output stream.
 * @param op        An asynchronous processing operation. Is invoked once
 *                  for each line of text in the input stream, and its
 *                  return value is written to the output stream. If the
 *                  function returns a non-string value then it is
 *                  converted to JSON before being written.
 * @param ns        (Optional) Name space for operation hook calls.
 * @param name      (Optional) Name for operation hook calls.
 * @param vars      (Optional) Context variables to be passed to the hook call.
 * @param opts      (Optional) Parsing options. Any of the following:
 *                  - multiValue: If true, then the transformer operation is a
 *                          one-to-many function that returns an array of multiple
 *                          values, with each value then written to a unique line 
 *                          in the output.
 *                  - separator: The character to use as the record separator.
 *                          Defaults to 0x0.
 * @return A promise which resolves once the input has been fully
 * processed.
 */
async function recordTransformer( ins, outs, op, ns, name, vars, opts = {} ) {
    const { multiValue = false, separator = 0x0 } = opts;
    // Lookup pre- and post- processor call hooks.
    const preHooks  = getCallHooks( ns, 'pre', name );
    const postHooks = getCallHooks( ns, 'post', name );
    // Process a line of text from the input.
    async function process( line ) {
        // Call any pre-process hooks.
        let input = await preHooks( line, vars );
        // Call the process operation.
        let result = await op( input );
        // Call any post-process hooks.
        result = await postHooks( result, vars );
        // Write the result to the output.
        if( result !== undefined ) {
            if( multiValue && Array.isArray( result ) ) {
                result.forEach( item => {
                    if( item !== undefined ) {
                        if( typeof item !== 'string' ) {
                            item = JSON.stringify( item );
                        }
                        outs.write( item );
                        outs.write('\n');
                    }
                });
            }
            else {
                if( typeof result !== 'string' ) {
                    result = JSON.stringify( result );
                }
                outs.write( result );
                outs.write('\n');
            }
        }
    }
    // Process the input.
    await recordParser( ins, process, separator );
}

/**
 * Multi-line input transformer. Takes as input a stream of text data,
 * extracts each line from the input and processes it using the op
 * function, before writing the result to an output stream.
 * @param ins       A readable input stream.
 * @param outs      A writeable output stream.
 * @param op        An asynchronous processing operation. Is invoked once
 *                  for each line of text in the input stream, and its
 *                  return value is written to the output stream. If the
 *                  function returns a non-string value then it is
 *                  converted to JSON before being written.
 * @param ns        (Optional) Name space for operation hook calls.
 * @param name      (Optional) Name for operation hook calls.
 * @param vars      (Optional) Context variables to be passed to the hook call.
 * @param multiValue: (Optional) If true, then the transformer operation is a
 *                  one-to-many function that returns an array of multiple
 *                  values, with each value then written to a unique line 
 *                  in the output.
 * @return A promise which resolves once the input has been fully
 * processed.
 */
async function lineTransformer( ins, outs, op, ns, name, vars, multiValue = false ) {
    const separator = NEWLINE;
    return recordTransformer( ins, outs, op, ns, name, vars, { multiValue, separator });
}

/**
 * JSON lines transformer. Takes as input a stream of multi-line text
 * data with one JSON object per line. Processes each JSON object using
 * the op function before writing the result to an output stream.
 * @param ins   A readable input stream.
 * @param outs  A writeable output stream.
 * @param op    An asynchronous processing operation.
 * @param ns    (Optional) Name space for operation hook calls.
 * @param name  (Optional) Name for operation hook calls.
 * @param vars  (Optional) Context variables to be passed to the hook call.
 * @return A promise which resolves once the input has been fully
 * processed.
 */
function jsonlTransformer( ins, outs, op, ns, name, vars ) {
    return lineTransformer( ins, outs, async ( line ) => {
        // Skip empty lines.
        if( line.length == 0 ) {
            return undefined;
        }
        let object = JSON.parse( line );
        let result = await op( object );
        return JSON.stringify( result );
    }, ns, name, vars );
}

module.exports = { lineTransformer, jsonlTransformer };

