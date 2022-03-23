package prov.idea.listeners;


import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import prov.idea.events.BGMEvent;



public class BindingPROVIDEA {
	private String id;
	private List<Entry<String,BGMEvent>> event;
	
	public BindingPROVIDEA(String id) {
		this.id = id;
		event = new ArrayList<Entry<String,BGMEvent>>();
	}

	public void addBinding(String type, BGMEvent e) {
		java.util.Map.Entry<String,BGMEvent> pair=new java.util.AbstractMap.SimpleEntry<>(type, e);
		event.add(pair);
	}
	
	public List<Entry<String,BGMEvent>> getBindings(){
		return event;
	}
	
}

