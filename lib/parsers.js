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

const NEWLINE = 0x0A;

/**
 * Parse a multi-field readable input stream.
 * @param ins       A readable stream containing data.
 * @param process   A process function, called once per field in the
 *                  input stream.
 * @param separator The character used to separate fields, for example
 *                  newline or 0x0.
 */
function fieldParser( ins, process, separator ) {
    return new Promise( ( resolve, reject ) => {
        // A buffer to hold input data.
        let buffer = Buffer.alloc( 0 );
        // A flag indicating whether we're still processing input.
        let processing = true;
        // Pending promises for unprocessed lines.
        let pending = Promise.resolve();
        // Append new data to the buffer.
        function append( data ) {
            if( !processing ) {
                return;
            }
            // Add data to the buffer.
            if( data ) {
                if( typeof data == 'string' ) {
                    data = Buffer.from( data );
                }
                buffer = Buffer.concat([ buffer, data ]);
            }
            // Input is complete if no data.
            let isComplete = !data;
            // Iterate over the data currently in the buffer looking
            // for newlines.
            let i, j, end = buffer.length - 1;
            for( i = 0, j = i; j < buffer.length; j++ ) {
                // If newline found, or if at end of input, then extract
                // and process the text line.
                if( buffer[j] == separator || (isComplete && j == end) ) {
                    let line = buffer.slice( i, j ).toString();
                    pending = pending.then( () => {
                        try {
                            return process( line );
                        }
                        catch( e ) {
                            processing = false;
                            pending = pending.then( () => reject( e ) );
                        }
                    });
                    // Reset start of next line.
                    i = j + 1;
                }
            }
            if( isComplete ) {
                processing = false;
                // If input is complete then finish processing once
                // all lines have been processed.
                pending.then( () => resolve() );
            }
            else {
                // Reset the buffer to only include non-processed data.
                buffer = buffer.slice( i );
            }
        }
        // Process the input.
        ins.on('data', append );
        ins.on('close', append );
    });
}

/**
 * Parse a multi-line readable text stream.
 * @param ins       A readable stream containing text content.
 * @param process   A process function, called once per line of text in the
 *                  input stream.
 */
function lineParser( ins, process ) {
    return fieldParser( ins, process, NEWLINE );
}

module.exports = { fieldParser, lineParser };

