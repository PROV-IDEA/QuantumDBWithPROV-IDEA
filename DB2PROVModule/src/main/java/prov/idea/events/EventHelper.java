package prov.idea.events;


import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.EventObject;
import java.util.HashMap;
import java.util.NoSuchElementException;


/**
 * Helper para el registro de listeners de tipo Listener gen�rico y para la
 * notificaci�n de los eventos a los listeners registrados.
 * <p>
 * Las clases que sean fuente de eventos de ejecuci�n podr�n usar objetos de
 * esta clase para realizar las tareas relacionadas con la gesti�n de los 
 * listener y la notificaci�n de los eventos.
 */
public class EventHelper<L> {
  // Lista de listeners suscritos
  private ArrayList<L> listeners;

  private HashMap<String, Method> methodTable;

  /**
   * El constructor recibe la clase del listener, para averiguar cuales son su 
   * m�todos por introspecci�n
   */
  public EventHelper(Class<L> interfaceListener) {
    initMethodTable(interfaceListener);
  }


private void initMethodTable(Class interfaceListener) {
    this.methodTable = new HashMap<>();
    Method[] methods = interfaceListener.getMethods();
    for (Method m : methods) {
      this.methodTable.put(m.getName(), m);
    }
  }

  /**
   * Suscribe un listener para que le sean notificados eventos
   */
  public synchronized void addListener(L l) {
    if (listeners == null) {
      listeners = new ArrayList();
    }

    if (!listeners.contains(l)) {
      listeners.add(l);
    }
  }

  /**
   * Elimina la suscripci�n de un listener
   */
  public synchronized void removeListener(L l) {
    if (listeners != null) {
      listeners.remove(l);
      if (listeners.size() == 0) {
        listeners = null;
      }
    }
  }

  /**
   * Devuelve la cantidad de listeners registrados
   */
  public synchronized int listenersSize() {
    if (listeners == null) {
      return 0;
    } else {
      return listeners.size();
    }
  }
  
  /**
   * Notifica el evento que digamos, con el objeto evento que digamos a los listeners registrados
   */
  public void fireEvent(String name, EventObject evt) throws InvocationTargetException {
    if (listeners == null) {
      return;
    }

    Method m = methodTable.get(name);
    if (m == null) {
      throw new NoSuchElementException("Los listeners gestionados por este helper no disponen del metodo " + name);
    }

    for (L listener : listeners) {
      try {
        m.invoke(listener, evt);
      } catch (IllegalAccessException ex) {
    	System.err.println("IllegalAccessException reaching listener: "+listener);
        //Logger.getLogger(EventHelper.class.getName()).log(Level.SEVERE, null, ex);
      }
    }
  }
}


