package com.imgraph.testing;

import com.imgraph.index.Tuple;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;

public class ChatActor extends UntypedActor {

	@SuppressWarnings("unchecked")
	public void onReceive(Object message) throws Exception {
		if(message instanceof Tuple) {
			ActorRef dest = getContext().actorFor("akka://ChatDaemon@"+((Tuple<String,String>) message).getX()+"/user/chat");
			final String string = ((Tuple<String,String>) message).getY();
			System.out.println("ChatActor is sending : " + string);
			dest.tell(string, getSelf());
		} else if (message instanceof String) {
			if (((String) message).equals("OK")){
				System.out.println("OK!");
			} else {
				System.out.println("ChatActor received String(\""+(String) message+"\") message!");
				final String string = "OK";
				getSender().tell(string);
			}
		} else {
			System.out.println("ELSE : " + message);
		}
	}

}
