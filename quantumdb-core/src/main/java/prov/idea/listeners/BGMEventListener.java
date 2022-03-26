package prov.idea.listeners;

import prov.idea.events.BGMEvent;

public interface BGMEventListener {
	
	public void newValueBinding(BGMEvent e);
	public void newBinding(BGMEvent e);
	public void operationStart(BGMEvent e);
	public void operationEnd(BGMEvent e);

}


