// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, test, vi } from 'vitest';
import userEvent from '@testing-library/user-event';
import { RegisterSynchronizerForm } from '../../../components/forms/RegisterSynchronizerForm';
import { Wrapper } from '../../helpers';
import {
  CREATE_PROPOSAL_LABEL_PROPOSAL_TYPE,
  PROPOSAL_SUMMARY_SUBTITLE,
} from '../../../utils/constants';

// The lookup needs an authenticated SV admin client, which the render wrapper does not set
// up, so the two sources the warning reads are mocked and the component's own logic tested.
const mockRegistration = vi.fn();
const mockVoteRequests = vi.fn();
vi.mock('../../../hooks/useSynchronizerRegistration', () => ({
  useSynchronizerRegistration: (synchronizerId: string, enabled: boolean) =>
    mockRegistration(synchronizerId, enabled),
}));
vi.mock('../../../hooks/useListVoteRequests', () => ({
  useListDsoRulesVoteRequests: () => mockVoteRequests(),
}));

const validSynchronizerId = 'dedicated::1220deadbeef';
const validOperator = 'operator::1220cafebabe';

beforeEach(() => {
  mockRegistration.mockReturnValue({ data: undefined });
  mockVoteRequests.mockReturnValue({ data: [] });
});

describe('Register Dedicated Synchronizer Form', () => {
  test('should render all Register Dedicated Synchronizer Form components', () => {
    render(
      <Wrapper>
        <RegisterSynchronizerForm />
      </Wrapper>
    );

    expect(screen.getByTestId('register-synchronizer-form')).toBeInTheDocument();
    expect(screen.getByText(CREATE_PROPOSAL_LABEL_PROPOSAL_TYPE)).toBeInTheDocument();

    const actionInput = screen.getByTestId('register-synchronizer-action');
    expect(actionInput.textContent).toBe('Register Dedicated Synchronizer');

    expect(screen.getByTestId('register-synchronizer-synchronizer-id')).toBeInTheDocument();
    expect(screen.getByTestId('register-synchronizer-operator')).toBeInTheDocument();
    expect(screen.getByTestId('register-synchronizer-discount-factor')).toBeInTheDocument();

    const summarySubtitle = screen.getByTestId('register-synchronizer-summary-subtitle');
    expect(summarySubtitle.textContent).toBe(PROPOSAL_SUMMARY_SUBTITLE);
  });

  test('should render errors when submit button is clicked on new form', async () => {
    const user = userEvent.setup();
    render(
      <Wrapper>
        <RegisterSynchronizerForm />
      </Wrapper>
    );

    const submitButton = screen.getByTestId('submit-button');
    await user.click(submitButton);
    expect(submitButton.getAttribute('disabled')).not.toBeNull();

    screen.getByText('Summary is required');
    screen.getByText('Invalid URL');

    // completing the form should reenable the submit button
    await user.type(screen.getByTestId('register-synchronizer-summary'), 'Register a sync');
    await user.type(screen.getByTestId('register-synchronizer-url'), 'https://example.com');
    await user.type(
      screen.getByTestId('register-synchronizer-synchronizer-id'),
      validSynchronizerId
    );
    await user.type(screen.getByTestId('register-synchronizer-operator'), validOperator);

    // the discount is prefilled with the no-discount value, so it never blocks submission
    await user.click(screen.getByTestId('register-synchronizer-action'));

    expect(submitButton.getAttribute('disabled')).toBeNull();
  });

  test('rejects a synchronizer id that is not name::fingerprint', async () => {
    const user = userEvent.setup();
    render(
      <Wrapper>
        <RegisterSynchronizerForm />
      </Wrapper>
    );

    const synchronizerIdInput = screen.getByTestId('register-synchronizer-synchronizer-id');
    await user.type(synchronizerIdInput, 'not-a-synchronizer-id');
    await user.click(screen.getByTestId('register-synchronizer-action'));

    screen.getByText('Invalid synchronizer id. Expected format: name::fingerprint');
  });

  test('prefills the discount with the no-discount value', () => {
    render(
      <Wrapper>
        <RegisterSynchronizerForm />
      </Wrapper>
    );

    expect(screen.getByTestId('register-synchronizer-discount-factor').getAttribute('value')).toBe(
      '1.0'
    );
  });

  test('bounds the discount factor to (0, 1]', async () => {
    const user = userEvent.setup();
    render(
      <Wrapper>
        <RegisterSynchronizerForm />
      </Wrapper>
    );

    const discountInput = screen.getByTestId('register-synchronizer-discount-factor');

    // the template's ensure rejects anything outside (0, 1], so the form does too
    await user.clear(discountInput);
    await user.type(discountInput, '1.5');
    await user.click(screen.getByTestId('register-synchronizer-action'));
    screen.getByText('Must be greater than 0 and at most 1');

    await user.clear(discountInput);
    await user.type(discountInput, '0.5');
    await user.click(screen.getByTestId('register-synchronizer-action'));
    expect(screen.queryByText('Must be greater than 0 and at most 1')).not.toBeInTheDocument();
  });
});

describe('Register Dedicated Synchronizer Form, duplicate warning', () => {
  const typeSynchronizerId = async (user: ReturnType<typeof userEvent.setup>) =>
    await user.type(
      screen.getByTestId('register-synchronizer-synchronizer-id'),
      validSynchronizerId
    );

  test('warns when the synchronizer id is already registered', async () => {
    mockRegistration.mockReturnValue({ data: { registration: {} } });
    const user = userEvent.setup();
    render(
      <Wrapper>
        <RegisterSynchronizerForm />
      </Wrapper>
    );

    await typeSynchronizerId(user);

    expect(screen.getByTestId('register-synchronizer-duplicate-warning').textContent).toContain(
      'already registered'
    );
  });

  test('warns when another open proposal asks for the same id', async () => {
    mockVoteRequests.mockReturnValue({
      data: [
        {
          payload: {
            action: {
              tag: 'ARC_DsoRules',
              value: {
                dsoAction: {
                  tag: 'SRARC_RegisterSynchronizer',
                  value: { synchronizerId: validSynchronizerId },
                },
              },
            },
          },
        },
      ],
    });
    const user = userEvent.setup();
    render(
      <Wrapper>
        <RegisterSynchronizerForm />
      </Wrapper>
    );

    await typeSynchronizerId(user);

    expect(screen.getByTestId('register-synchronizer-duplicate-warning').textContent).toContain(
      'Another open proposal'
    );
  });

  test('does not warn for an id that is neither registered nor proposed', async () => {
    const user = userEvent.setup();
    render(
      <Wrapper>
        <RegisterSynchronizerForm />
      </Wrapper>
    );

    await typeSynchronizerId(user);

    expect(screen.queryByTestId('register-synchronizer-duplicate-warning')).not.toBeInTheDocument();
  });

  test('does not look up an id that is not yet well formed', async () => {
    const user = userEvent.setup();
    render(
      <Wrapper>
        <RegisterSynchronizerForm />
      </Wrapper>
    );

    await user.type(screen.getByTestId('register-synchronizer-synchronizer-id'), 'not-an-id');

    // the lookup is gated on the id parsing, so it is never asked about a malformed one
    expect(mockRegistration).toHaveBeenCalledWith('not-an-id', false);
  });
});
