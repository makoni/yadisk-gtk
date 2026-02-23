use std::collections::VecDeque;

use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub enum OperationKind {
    Upload,
    Download,
    Delete,
    Move,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Operation {
    pub kind: OperationKind,
    pub path: String,
    pub payload: Option<String>,
    pub attempt: u32,
    pub retry_at: Option<i64>,
    pub priority: i32,
}

#[derive(Debug, Error)]
pub enum QueueError {
    #[error("operation queue is empty")]
    Empty,
}

#[derive(Debug, Default)]
pub struct OperationQueue {
    inner: VecDeque<Operation>,
}

impl OperationQueue {
    #[allow(dead_code)]
    pub fn enqueue(&mut self, op: Operation) {
        self.inner.push_back(op);
    }

    #[allow(dead_code)]
    pub fn dequeue(&mut self) -> Result<Operation, QueueError> {
        self.inner.pop_front().ok_or(QueueError::Empty)
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fifo_order_is_preserved() {
        let mut queue = OperationQueue::default();
        queue.enqueue(Operation {
            kind: OperationKind::Upload,
            path: "/A".into(),
            payload: None,
            attempt: 0,
            retry_at: None,
            priority: 0,
        });
        queue.enqueue(Operation {
            kind: OperationKind::Download,
            path: "/B".into(),
            payload: None,
            attempt: 0,
            retry_at: None,
            priority: 0,
        });

        let first = queue.dequeue().unwrap();
        let second = queue.dequeue().unwrap();

        assert_eq!(first.path, "/A");
        assert_eq!(second.path, "/B");
        assert!(queue.is_empty());
    }

    #[test]
    fn dequeue_on_empty_returns_error() {
        let mut queue = OperationQueue::default();
        assert!(matches!(queue.dequeue(), Err(QueueError::Empty)));
    }
}
