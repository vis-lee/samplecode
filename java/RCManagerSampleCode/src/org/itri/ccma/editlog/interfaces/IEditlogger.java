package org.itri.ccma.editlog.interfaces;

import java.io.IOException;

public interface IEditlogger {
	public void log(long runtimeID, byte logType, byte[] data)
			throws IOException;

	public void shutdown();
}
