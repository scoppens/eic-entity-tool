/**
 * Copyright 2011, Campinas Stephane
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/**
 * @project trec-entity-tool
 * @author Campinas Stephane [ 9 Jun 2011 ]
 * @link stephane.campinas@deri.org
 */
package org.sindice.siren.index;

import java.io.File;
import java.io.IOException;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.solr.client.solrj.SolrServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 */
public class IndexingCLI {

  private static final Logger   logger      = LoggerFactory.getLogger(IndexingCLI.class);
  
  private final OptionParser    parser;
  private OptionSet             opts;

  private final String          HELP        = "help";
  private final String          DUMPS_DIR   = "dumps-dir";
  private final String          INDEX_URL   = "index-URL";
  private final String          FORMAT      = "format";
  private final String          COMMIT      = "commit";
  private final String          STORE       = "store";
  private final String          SKIP_TO     = "skip-to";
  private final String 			CLEAR		= "clear";
  
  private File dumpsDir;
  private String indexURL;
  private Format format;
  
  public static enum Format {
    SINDICE_ED, SINDICE_DE
  }
  
  /**
   * 
   */
  public IndexingCLI() {
    parser = new OptionParser();
    parser.accepts(HELP, "Print this help.");
    parser.accepts(DUMPS_DIR, "The directory with Sindice-ED or Sindice-DE dumps files")
          .withRequiredArg().ofType(File.class);
    parser.accepts(INDEX_URL, "The SIREN server URL where the index will be written to.")
          .withRequiredArg().ofType(String.class);
    parser.accepts(FORMAT, "The dataset format, either SINDICE_ED or SINDICE_DE")
          .withRequiredArg().ofType(Format.class);
    parser.accepts(COMMIT, "Commit documents by batch of X")
          .withRequiredArg().ofType(Integer.class).defaultsTo(IndexingMMLab.COMMIT);
    parser.accepts(STORE, "Store the triples, incomings and outogings, of entities");
    parser.accepts(SKIP_TO, "Skips over the dumps strictly lower than X, e.g., ED-00010.tar.gz is filtered if 10 < X.")
          .withRequiredArg().ofType(Integer.class).defaultsTo(0);
    parser.accepts(CLEAR, "Clear the index prior to the ingest");
    
    	ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
        root.setLevel(ch.qos.logback.classic.Level.INFO);
    
  }
  
  private void printError(final String opt)
  throws IOException {
    parser.printHelpOn(System.out);
    throw new IOException("Missing option: " + opt);
  }
  
  public final void parseAndExecute(final String[] cmds)
  throws IOException, SolrServerException {
    opts = parser.parse(cmds);
    if (opts.has(HELP)) {
      parser.printHelpOn(System.out);
      return;
    }
    
    IndexingMMLab.STORE = opts.has(STORE);
    IndexingMMLab.CLEAR = opts.has(CLEAR);
    IndexingMMLab.COMMIT = (Integer) opts.valueOf(COMMIT);
    IndexingMMLab.SKIP_TO = (Integer) opts.valueOf(SKIP_TO);
    
    // FORMAT
    if (opts.has(FORMAT)) {
      format = (Format) opts.valueOf(FORMAT);
    } else
      printError(FORMAT);
    
    // DUMPS_DIR
    if (opts.has(DUMPS_DIR)) {
      dumpsDir = (File) opts.valueOf(DUMPS_DIR);
    } else
      printError(DUMPS_DIR);
    
    // INDEX_URL
    if (opts.has(INDEX_URL)) {
      indexURL = (String) opts.valueOf(INDEX_URL);
    } else
      printError(INDEX_URL);
    
    logger.info("Creating index at {} from the files at {}", indexURL , dumpsDir.getAbsolutePath());
    final IndexingMMLab indexing;
    switch (format) {
      case SINDICE_DE:
        indexing = new SindiceDEIndexing(dumpsDir, indexURL);
        break;
      case SINDICE_ED:
        indexing = new SindiceEDIndexing(dumpsDir, indexURL);
        break;
      default:
        throw new IllegalArgumentException("No such dataset format: " + format);
    }
    indexing.indexIt();
    logger.info("Finished indexing");
  }
  
  public static void main(String[] args)
  throws IOException, SolrServerException {
    final IndexingCLI cli = new IndexingCLI();
    
    cli.parseAndExecute(args);
  }
  
}
