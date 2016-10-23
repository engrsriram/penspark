package com.penspark;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class CmdLine {
 private static final Logger log = Logger.getLogger(CmdLine.class.getName());
 private String[] args = null;
 private Options options = new Options();

 public CmdLine(String[] args) {

  this.args = args;
	/*
	/rep            : Repository name
	/user           : Repository username
	/pass           : Repository password
	/trans          : The name of the transformation to launch
	/dir            : The directory (dont forget the leading /)
	/file           : The filename (Transformation in XML) to launch
	/level          : The logging level (Basic, Detailed, Debug, Rowlevel, Error, Nothing)
	/logfile        : The logging file to write to
	/listdir        : List the directories in the repository
	/listtrans      : List the transformations in the specified directory
	/listrep        : List the available repositories
	/exprep         : Export all repository objects to one XML file
	/norep          : Do not log into the repository
	/safemode       : Run in safe mode: with extra checking enabled
	/version        : show the version, revision and build date
	/param          : Set a named parameter <NAME>=<VALUE>. For example -param:FOO=bar
	/listparam      : List information concerning the defined named parameters in the specified transformation.
	/maxloglines    : The maximum number of log lines that are kept internally by Kettle. Set to 0 to keep all rows (default)
	/maxlogtimeout  : The maximum age (in minutes) of a log line while being kept internally by Kettle. Set to 0 to keep all rows indefinitely (default)
	*/
  
  options.addOption("rep",            false, " Repository name");
  options.addOption("user",           false, " Repository username");
  options.addOption("pass",           false, " Repository password");
  options.addOption("trans",          false, " The name of the transformation to launch");
  options.addOption("dir",            false, " The directory (dont forget the leading /)");
  options.addOption("file",           true, " The filename (Transformation in XML) to launch");
  options.addOption("level",          false, " The logging level (Basic, Detailed, Debug, Rowlevel, Error, Nothing)");
  options.addOption("logfile",        false, " The logging file to write to");
  options.addOption("listdir",        false, " List the directories in the repository");
  options.addOption("listtrans",      false, " List the transformations in the specified directory");
  options.addOption("listrep",        false, " List the available repositories");
  options.addOption("exprep",         false, " Export all repository objects to one XML file");
  options.addOption("norep",          false, " Do not log into the repository");
  options.addOption("safemode",       false, " Run in safe mode: with extra checking enabled");
  options.addOption("version",        false, " show the version, revision and build date");
  options.addOption("param",          false, " Set a named parameter <NAME>=<VALUE>. For example -param:FOO=bar");
  options.addOption("listparam",      false, " List information concerning the defined named parameters in the specified transformation.");
  options.addOption("maxloglines",    false, " The maximum number of log lines that are kept internally by Kettle. Set to 0 to keep all rows (default)");
  options.addOption("maxlogtimeout",  false, " The maximum age (in minutes) of a log line while being kept internally by Kettle. Set to 0 to keep all rows indefinitely (default)");

 // options.addOption("h", "help", false, "show help.");
  //options.addOption("v", "var", true, "Here you can set parameter .");

 }

 public String getFilename() {
  CommandLineParser parser = new GnuParser();

  CommandLine cmd = null;
  try {
   cmd = parser.parse(options, args);

   if (cmd.hasOption("file")) {
   //log.log(Level.INFO, "Using file " + cmd.getOptionValue("file"));
    // Whatever you want to do with the setting goes here
   } else {
    log.log(Level.SEVERE, "Input File Missing");
    help();
   }

  } catch (ParseException e) {
   log.log(Level.SEVERE, "Failed to parse comand line properties", e);
   help();
  }
  return cmd.getOptionValue("file");
 }

 private void help() {
  // This prints out some help
  HelpFormatter formater = new HelpFormatter();

  formater.printHelp("Main", options);
  System.exit(0);
 }
}