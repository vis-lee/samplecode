package org.itri.ccma.editlog.manager2;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.itri.ccma.editlog.interfaces.IEditlogModule;

public class EditlogEngine {
	private static final Log LOG = LogFactory.getLog(EditlogEngine.class);
	private static boolean isRunning = false;
	private static boolean isLogging = false;
	private static String editlogFolder = "";//EditlogConfigurables.DEFAULT_EDITLOG_FOLDER;

	synchronized public static boolean registerModule(byte type, IEditlogModule module, String moduleName) {
		return isLogging;
	}

	private static boolean recoverEngine() throws Exception {
		return isLogging;
	}

	synchronized public static void startEngine() throws Exception {
	}

	synchronized public static void startEngine(String setEditlogFolderPath) throws Exception {
	}

	private static void startCore(String setEditlogFolderPath) throws Exception {
	}

	synchronized public static void startEngineWithoutLogging() throws Exception {
	}

	synchronized public static void stopEngine() {
	}

	static boolean isRunning() {
		return isRunning;
	}

	static boolean isLogging() {
		return isLogging;
	}

	public static void fortest_running() {
	}

	public static void fortest_stopping() {
	}

	static String getEditlogFolderPath() {
		return editlogFolder;
	}

	private static void setEditlogFolderPath(String folderPath) {
		editlogFolder = folderPath;
	}
}
