"""
Routes for due order management in the ASR system.

This module provides API endpoints for identifying, analyzing, and managing
due orders based on service level requirements.
"""
from flask import Blueprint, jsonify, request, current_app
from sqlalchemy import and_

from models.order import Order, OrderStatus, OrderCategory
from models.source import Source
from services.due_order import (
    identify_due_orders, 
    is_service_due_order, 
    get_order_delay,
    calculate_projected_service_impact
)
from utils.db import get_session

due_order_bp = Blueprint('due_order', __name__, url_prefix='/api/due-orders')

@due_order_bp.route('/', methods=['GET'])
def get_due_orders():
    """Get all due orders based on service level requirements."""
    try:
        session = get_session()
        
        # Get filter parameters
        buyer_id = request.args.get('buyer_id')
        store_id = request.args.get('store_id')
        
        # Identify due orders
        due_orders = identify_due_orders(session, buyer_id, store_id)
        
        session.close()
        
        return jsonify({
            'success': True,
            'due_orders': due_orders,
            'count': len(due_orders)
        })
    
    except Exception as e:
        current_app.logger.error(f"Error getting due orders: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@due_order_bp.route('/check/<source_id>/<store_id>', methods=['GET'])
def check_if_due(source_id, store_id):
    """Check if an order for a specific source and store is due."""
    try:
        session = get_session()
        
        # Get source ID
        source = session.query(Source).filter(Source.source_id == source_id).first()
        
        if not source:
            session.close()
            return jsonify({
                'success': False,
                'error': f"Source {source_id} not found"
            }), 404
        
        # Check if order is due
        is_due, details = is_service_due_order(session, source.id, store_id)
        
        # Get order delay
        order_delay = get_order_delay(session, source.id, store_id)
        
        # Get impact if delayed by 1 day
        service_impact = calculate_projected_service_impact(session, source.id, store_id, 1)
        
        session.close()
        
        return jsonify({
            'success': True,
            'is_due': is_due,
            'reason': details.get('reason'),
            'details': details,
            'order_delay': order_delay,
            'service_impact': service_impact
        })
    
    except Exception as e:
        current_app.logger.error(f"Error checking if order is due: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@due_order_bp.route('/simulate-delay/<source_id>/<store_id>', methods=['GET'])
def simulate_delay(source_id, store_id):
    """Simulate the impact of delaying an order by a specified number of days."""
    try:
        session = get_session()
        
        # Get delay days from query parameters (default to 1)
        delay_days = int(request.args.get('days', 1))
        
        # Get source ID
        source = session.query(Source).filter(Source.source_id == source_id).first()
        
        if not source:
            session.close()
            return jsonify({
                'success': False,
                'error': f"Source {source_id} not found"
            }), 404
        
        # Calculate service impact
        service_impact = calculate_projected_service_impact(session, source.id, store_id, delay_days)
        
        session.close()
        
        return jsonify({
            'success': True,
            'delay_days': delay_days,
            'service_impact': service_impact
        })
    
    except Exception as e:
        current_app.logger.error(f"Error simulating order delay: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@due_order_bp.route('/set-due/<order_id>', methods=['POST'])
def set_order_due(order_id):
    """Manually set an order as due."""
    try:
        session = get_session()
        
        # Get order
        order = session.query(Order).filter(Order.id == order_id).first()
        
        if not order:
            session.close()
            return jsonify({
                'success': False,
                'error': f"Order {order_id} not found"
            }), 404
        
        # Set as due
        order.status = OrderStatus.DUE
        order.category = OrderCategory.DUE
        
        # Get reason from request data
        data = request.get_json()
        reason = data.get('reason', 'manual')
        
        # Commit changes
        session.commit()
        session.close()
        
        return jsonify({
            'success': True,
            'message': f"Order {order_id} set as due",
            'reason': reason
        })
    
    except Exception as e:
        current_app.logger.error(f"Error setting order as due: {e}")
        session.rollback()
        session.close()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@due_order_bp.route('/update-delay/<order_id>', methods=['GET'])
def update_order_delay(order_id):
    """Update the delay for a specific order."""
    try:
        session = get_session()
        
        # Get order
        order = session.query(Order).filter(Order.id == order_id).first()
        
        if not order:
            session.close()
            return jsonify({
                'success': False,
                'error': f"Order {order_id} not found"
            }), 404
        
        # Calculate order delay
        delay = get_order_delay(session, order.source_id, order.store_id)
        
        # Update order
        order.order_delay = delay
        
        # Commit changes
        session.commit()
        session.close()
        
        return jsonify({
            'success': True,
            'order_id': order_id,
            'order_delay': delay
        })
    
    except Exception as e:
        current_app.logger.error(f"Error updating order delay: {e}")
        session.rollback()
        session.close()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@due_order_bp.route('/run-nightly-check', methods=['POST'])
def run_nightly_check():
    """Run the nightly check to identify and update all due orders."""
    try:
        session = get_session()
        
        # Identify all due orders
        due_orders = identify_due_orders(session)
        
        # Update order status and category for due orders
        for due_order in due_orders:
            order_id = due_order['order_id']
            order = session.query(Order).filter(Order.id == order_id).first()
            
            if order:
                order.status = OrderStatus.DUE
                order.category = OrderCategory.DUE
        
        # Update delay for all non-due orders
        non_due_orders = session.query(Order).filter(
            and_(
                Order.status != OrderStatus.DUE,
                Order.status != OrderStatus.ACCEPTED,
                Order.status != OrderStatus.PURGED,
                Order.status != OrderStatus.DEACTIVATED
            )
        ).all()
        
        for order in non_due_orders:
            order.order_delay = get_order_delay(session, order.source_id, order.store_id)
        
        # Commit changes
        session.commit()
        session.close()
        
        return jsonify({
            'success': True,
            'due_orders': len(due_orders),
            'non_due_orders': len(non_due_orders),
            'total_updated': len(due_orders) + len(non_due_orders)
        })
    
    except Exception as e:
        current_app.logger.error(f"Error running nightly due order check: {e}")
        session.rollback()
        session.close()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500