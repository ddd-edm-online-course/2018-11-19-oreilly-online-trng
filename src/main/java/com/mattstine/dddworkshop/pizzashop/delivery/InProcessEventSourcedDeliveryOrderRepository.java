package com.mattstine.dddworkshop.pizzashop.delivery;

import com.mattstine.dddworkshop.pizzashop.infrastructure.events.ports.EventLog;
import com.mattstine.dddworkshop.pizzashop.infrastructure.events.ports.Topic;
import com.mattstine.dddworkshop.pizzashop.infrastructure.repository.adapters.InProcessEventSourcedRepository;
import com.mattstine.dddworkshop.pizzashop.kitchen.KitchenOrderRef;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Matt Stine
 */
final class InProcessEventSourcedDeliveryOrderRepository extends InProcessEventSourcedRepository<DeliveryOrderRef, DeliveryOrder, DeliveryOrder.OrderState, DeliveryOrderEvent, DeliveryOrderAddedEvent> implements DeliveryOrderRepository {

	private final Map<KitchenOrderRef, DeliveryOrderRef> kitchenOrderRefToDeliveryOrderRef;

	InProcessEventSourcedDeliveryOrderRepository(EventLog eventLog, Topic topic) {
		super(eventLog,
				DeliveryOrderRef.class,
				DeliveryOrder.class,
				DeliveryOrder.OrderState.class,
				DeliveryOrderAddedEvent.class,
				topic);

		kitchenOrderRefToDeliveryOrderRef = new HashMap<>();

		eventLog.subscribe(topic, e -> {
			if (e instanceof DeliveryOrderAddedEvent) {
				kitchenOrderRefToDeliveryOrderRef.put(((DeliveryOrderAddedEvent) e)
								.getState()
								.getKitchenOrderRef(),
						((DeliveryOrderAddedEvent) e).getRef());
			}
		});
	}

	@Override
	public DeliveryOrder findByKitchenOrderRef(KitchenOrderRef kitchenOrderRef) {
		DeliveryOrderRef deliveryOrderRef = kitchenOrderRefToDeliveryOrderRef.get(kitchenOrderRef);
		DeliveryOrder deliveryOrder = findByRef(deliveryOrderRef);
		if (deliveryOrder != null) {
			return deliveryOrder;
		}
		return null;
	}
}
