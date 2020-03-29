package org.itri.ccma.editlog.interfaces;

public interface IEditlogModule {
	public void undo(byte[] data) throws Exception;

	public void redo(byte[] data) throws Exception;

	public boolean flushTimeout(long flushElement);
}
