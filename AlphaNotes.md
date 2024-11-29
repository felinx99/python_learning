# Alpha Notes

### [Alpha Heuristic for Leverage / Scaling (Quantopian)](https://www.quantopian.com/posts/enhancing-short-term-mean-reversion-strategies-1)
### 用于杠杆/规模化交易的 Alpha 启发式方法 (Quantopian)

## 以下是中文翻译

#### Guy Fleury 2017年3月17日
@Anthony，你从通常的公式开始：A(t) = A(0)∙(1 + r)^t，其中 r 被视为平均市场回报率，你可以通过购买 SPY、DIA 或 QQQ 并持有到期来获得。

你想要设计一个可扩展的交易策略：A(t) = k∙A(0)∙(1 + r)^t。请注意，购买 k 个 SPY 并持有是 100% 可扩展的。这也是我设计我的交易策略可扩展的原因。否则，如果它们不可扩展，它们怎么可能达到管理大型投资组合的水平呢？

你想要交易，那么你就必须跑赢平均水平。将一些 alpha 值添加到组合中。这将导致：A(t) = k∙A(0)∙(1 + r + α)^t，其中 α 表示你的交易技巧为问题带来的溢价或贡献。

显然，你的技能需要产生积极影响 (α > 0)。尽管如此，你仍然希望策略是可扩展的。正如之前观察到的，这种特定的交易策略向下扩展性并不强。如果你投入少量资金，它只会摧毁账户。它向上扩展的幅度也只限于部分。但即便如此，它仍然可以产生一些 alpha 值。积极的 alpha 值转化为跑赢平均水平，产生更多的资金，这正是整个游戏的目的。

IB 的杠杆费用低于 3%，但杠杆只针对超过你净值的金额。只有当杠杆超过 1 时；L > 1.00，你才会被收取超额利息。因此，你可以使用以下公式进行近似计算：A(t) = k∙A(0)∙(1 + r + α – 0.03)^t。使用杠杆就像增加投注规模一样，因此它会对整体分数产生影响，得到：A(t) = (1 + L)∙k∙A(0)∙(1 + r + α - 0.03)^t。

策略没有使用 100% 的杠杆，而是使用了 60%。毕竟，它被设计成一个 130/30 的对冲基金。这将把杠杆估计值降低到：A(t) = (1 + L)∙k∙A(0)∙(1 + r + α – 0.03*0.60)^t。

通过修改 Blue 的程序 (两个数字)，我得到了：r + α = 21.87%，L = 0.60，A(0) = $1M，和 t = 14.36 年。

我的模拟将 k 设置为 5，因为我想看看该策略的可扩展性程度。在这方面，交易策略的响应并不理想。这是可以理解的，因为它没有对回报随时间推移而下降进行任何补偿。但是，这是一个可以纠正的缺陷，并且这样做会进一步提高性能。


#### Guy Fleury 2017年3月18日
@Anthony，使用的公式非常基础。它考虑的是终点，而不是到达那里的路径。尽管如此，终点给出相同的数字，就像一个假设。

A(t) 代表投资组合的持续净清算价值（其资产总和）。你也可以用以下公式表示同样的意思：A(t) = A(0) + Ʃ ƒ H(t)∙dP，读起来是一样的。A(t) 是初始资本的总和，再加上所有交易的最终结果。

A(0) 是初始投资组合价值（通常是可用的初始交易资本）。

r 是复合年增长率 (CAGR)。这里，r 表示平均市场回报率。它也可以作为长期投资（例如超过 10 年）的人最期望的投资组合结果。

t 代表时间（单位为年）。在本例中，t 为 14.36 年。

k 只是一个缩放因子。如果你投入 2 倍的初始资本 (2∙A(0))，你应该期望获得 2 倍的总利润。这取决于策略的可扩展性。

alpha α 的用法与 Jensen 在 60 年代后期提出的用法相同。它是指添加到游戏中的技能。积极的 alpha 值意味着你在管理投资组合方面所做的工作是有益的，因为它增加了复合年增长率。

Jensen 在他的开创性论文中观察到，平均的投资组合经理的 alpha 值为负。这意味着他们所做的事情，从长远来看，平均而言会损害他们的整体业绩，因为他们中的大多数最终都未能超越平均水平。你的交易脚本需要产生正的 alpha 值 (α > 0)，否则你的投资组合可能会落入上述类别。

L 代表杠杆。对于 L < 1.00，没有杠杆费用，因为它相当于没有完全投资。在 Blue 的程序中，如果你想要 2.00 的杠杆，你可以更改一个或两个数字来实现。正如所说，杠杆作用与增加投注规模相同。你为这种能力付费。

你希望你的程序能够实现 α > lc，即其杠杆费用 (lc)。除非 α > 2∙lc，即额外 alpha 值至少是杠杆费用的两倍，否则我不会认为杠杆交易策略有趣。否则，你可能是在为经纪人而不是为自己交易。而且，如你所知，如果交易策略行为不当或控制不佳，结果可能会更糟。

因此，使用所呈现的方程，可以在执行不同场景之前估计其结果，并了解对总回报的影响。如果你设置 k = 5，这意味着 5 倍的初始资本，你应该期望获得 5 倍的利润。如果你没有得到它，那么你的程序在可扩展性方面存在弱点。这应该导致寻找方法来补偿性能下降。

#### Guy Fleury 2017年3月25日
@Grant，我认为我们只是有不同的观点。

对我来说，自动交易策略需要向上可扩展。如果不是，它的价值就会大大降低。

如果无论使用何种交易方法，在长期内产生的收益都低于市场平均水平，那么该策略就毫无价值，因为购买指数基金的替代方案将以更少的努力产生更多的收益。

没有人会投入 1,000 个 10,000 美元的策略（1000 万美元）。特别是如果你有 10 倍或 100 倍以上的资本要投资。你需要可扩展的策略，这些策略不会在资产管理规模压力下崩溃。你还需要能够处理数据和执行交易的机器。你需要一个不仅可行，而且可行的场景，能够处理整个交易/投资区间内的 A(t)。这个交易区间不能仅仅是几年，否则你只是在进行时间赌博，你激活账户的时期变得最重要。

此外，正如你在这里多次看到的那样，拥有一个 10,000 美元的策略绝不保证在 10 万美元、100 万美元或 1000 万美元的情况下是可行的。每个级别的遇到的问题都非常不同，这仅仅是基于投入的初始资本。

我曾经给出过这个公式：A(t) = (1+L)∙k∙A(0)∙(1 + r + α - lc% - fc%)^t，我认为理想的情况是期望一个 50/50 的多空组合具有接近零的贝塔系数的正 alpha 值。在 Q 上展示的例子未能通过这个严苛的测试：α > 0。而且，当它们被放大时，通常会变得更糟：k∙A(0)，增加交易区间，考虑摩擦成本 fc% 和杠杆费用 lc%。

有三种方法可以提高夏普比率。一种是真正提高投资组合的平均回报率（产生一些 alpha）。另一种是降低波动性。另一种是同时做到这两点。然而，无论何时想要降低波动性，平均而言，风险会降低，但回报也会降低。

因此，对于那些希望获得一些 Q 配置甚至只是可行的交易策略的人来说，他们应该了解该公式的影响：A(t) = (1+v∙L)∙k∙A(0)∙(1 + r + α – v∙lc% – v∙fc%)^t，其中 r，市场平均水平，将因负 alpha α 而降低。杠杆费用和摩擦成本将是杠杆提升的 v 倍。显然，如果 v = 1（无提升），k = 1（无缩放），lc% = 0（无杠杆），你将剩下：A(t) = A(0)∙(1 + r + α – fc%)^t，因为佣金仍需支付。然后，你最好有一些交易技能来超越平均水平（α > fc%）。

我不认为这是一个非常漂亮的照片。唯一的补救措施是拥有足够大的正 alpha 值来补偿所有缺点。即使是这里很少讨论的一个，即长期回报下降。

我是一个局外人，我永远不会在没有对足够大的投资组合进行长时间测试的情况下使用任何人的策略，这就是我使用 Q 的方式，探索它们的极限，看看它们能走多远，在哪里会崩溃？

就像如果你不能向我展示你的交易策略是向上可扩展的，它的价值是什么？如果你能证明它能产生一些 alpha，那么它的未来价值又是什么？如果它不可杠杆，我们如何进一步推动它做得更多？

对我来说，你使用哪种交易方法并不重要，我仅对 A(t) 感兴趣。A(t) 的得分记录在交易账户的底线。

无论你想如何看待它。如果你想要更多，你必须比其他人做得更多。

就像如果我们有完全相同的汽车，并进行一场长途比赛到终点线。我们各自的驾驶技巧将很重要。但无论如何，我只需更换轮胎就可以获得明显的优势。然后驾驶技巧可能就不那么重要了。
## 以下是英文原谅
#### Guy Fleury Mar 17, 2017

> @Anthony, you start with the usual: A(t) = A(0)∙(1 + r)^t where r is viewed as the average market return which you can have just by buying SPY, DIA, or QQQ, and holding on for the duration.
>
> You want to design a trading strategy that is scalable: A(t) = k∙A(0)∙(1 + r)^t. Note that buying k SPY and holding is scalable 100%. It is also why I design my trading strategies to be scalable. Otherwise, how could they ever reach the level of managing a large portfolio if they were not scalable.
>
> You want to trade, then you have to outperform the average. Bring some alpha to the mix. This will result in: A(t) = k∙A(0)∙(1 + r + α)^t where α represent the premium or contribution your trading skills bring to the problem.
>
> Evidently, your skills needs to have a positive impact (α > 0). Nonetheless, you still want the strategy to be scalable. As previously observed, this particular trading strategy is not that scalable down. If you go small, it will simply destroy the account. It is only partially scalable up. But, still, it can generate some alpha. And, positive alpha translate to outperforming averages, generating more money which was the whole purpose of the game.
>
> IB charges less than 3% for leverage, but leverage is only for the sum exceeding your equity. It is only when leverage exceeds one; L > 1.00 that you will be charged interests on the excess. So you can get an approximation using: A(t) = k∙A(0)∙(1 + r + α – 0.03)^t. Using leverage is the same as increasing the bet size and therefore it will have an impact on the overall score, giving: A(t) = (1 + L)∙k∙A(0)∙(1 + r + α - 0.03)^t.
>
> The strategy was not using 100% leverage, but 60%. It was designed as a 130/30 hedge fund after all. This would have for impact to reduce the leveraging estimate to:
> A(t) = (1 + L)∙k∙A(0)∙(1 + r + α – 0.03\*0.60)^t.
>
> With the modifications to Blue's program (two numbers) I got: r + α = 21.87%, L = 0.60, A(0) = \$1M, and t = 14.36 years.
>
> My simulation did put k = 5 since I wanted to see the extent of the strategy's scalability. On that front, the trading strategy did not respond that well. It is understandable, it does not make any compensation for return degradation over time. But, that is a flaw that can be corrected, and doing so will improve performance even more.

#### Guy Fleury Mar 18, 2017

> @Anthony, the formula used is very basic. It considers the end points, not the path to get there. Nonetheless, the end points give the same numbers, an as if.
>
> A(t) represents the ongoing net liquidating value of a portfolio (sum of its assets). You could also express the same thing using:
> A(t) = A(0) + Ʃ ƒ H(t)∙dP which reads the same. A(t) is the sum of the initial capital to which is added the end results of all trades.
>
> A(0) is the initial portfolio value (usually the available initial trading capital).
>
> r is the compounded annual growth rate (CAGR). Here, r is expressed as the average market return. It can also serve as the most expected portfolio outcome for someone playing for the long term, say more than 10 years.
>
> t is for the time in years. In this case t was for 14.36 years.
>
> k is just a scaling factor. You put 2 times more in initial capital (2∙A(0)), you should expect 2 times more in total profits. This, if the strategy is scalable.
>
> The alpha α is used in the same way as Jensen presented it in the late 60's. It is for the skills brought to the game. A positive alpha meant that what you did in managing a portfolio was beneficial since it was adding to the CAGR.
>
> What Jensen observed in his seminal paper was that the average portfolio manager had a negative alpha. Meaning that what they did, on average, was detrimental to their overall long term performance since the majority of them ended up not beating the averages. Your trading script needs to generate a positive alpha (α > 0), otherwise your portfolio might fall in the above cited category.
>
> L is for the leverage. There are no leveraging fees for L < 1.00 since it is equivalent to be not fully invested. In Blue's program, if you wanted a leverage of 2.00, you could change one, or two numbers to do the job. As was said, leveraging has the same impact as increasing the bet size. You pay for the ability of doing so.
>
> What you want your program to do is have your α > lc, its leveraging charge (lc). I don't find leveraging a trading strategy interesting unless it has an α > 2∙lc, meaning the added alpha is at least twice the leveraging charges. Otherwise you might be trading for the benefit of the broker, not your own. And as you know, the result could be even worse if the trading strategy is not well behaved or well controlled.
>
> So, with the presented equation, one can estimate the outcome of different scenarios even before performing them knowing what will be the impact on total return. If you set k = 5, meaning 5 times more initial capital, you should expect 5 times more profits. If you don't get it, then your program is showing weaknesses in the scalability department. This should lead to finding ways to compensate for the performance degradation.

#### Guy Fleury Mar 25, 2017

> @Grant, I think we simply have a different viewpoint.
>
> For me, an automated trading strategy needs to be upward scalable. If not, its value is considerably reduced.
>
> If scaling up for whatever trading methods used gets to produce over the long haul less than market average, then a strategy becomes literally worthless since the alternative of buying an index fund would have generated more for a lot less work.
>
> No one will put 1,000 $10k strategies to work ($10M). Especially if you have 10 times or 100 times more capital to invest. You need scalable strategies that will not break down under AUM pressure. You also need machines that can handle the data and execute the trades. You need not only a doable, but also a feasible scenario that can handle A(t) for the entire trading/investing interval. This trading interval can not be just for a few years, otherwise you are just taking a timing bet where the period you activate your account becomes the most important.
>
> Also, as you must have seen numerous times here, having a $10k strategy is by no means a guaranteed viable scenario at $100k, $1M, or $10M for that matter. The problems encountered at each level are quite different, and this just based on the initial capital put to work.
>
> I've given this formula elsewhere: `A(t) = (1+L)∙k∙A(0)∙(1 + r + α - lc% - fc%)^t`, and what I see as dreamland is expecting some positive alpha for a 50/50 long short portfolio with a near zero beta. The examples that have been presented on Q failed that acid test: `α > 0`. And it usually gets worse when you scale them up: k∙A(0), increase the trading interval, account for frictional costs fc%, and leveraging fees lc%.
>
> There are 3 ways to improve the Sharpe ratio. One is to really increase your portfolio average return (generate some alpha). Another is to reduce volatility. And the other is to do both at the same time. However, anytime you want to reduce volatility, it entails on average, lower risks but also lower returns.
>
> So, for someone wishing some Q allocation or even just a viable trading strategy, they should understand the implications of the formula: `A(t) = (1+v∙L)∙k∙A(0)∙(1 + r + α – v∙lc% – v∙fc%)^t` where r, the market average, will be reduced by a negative alpha α. And where leveraging fees and frictional costs will be v times the leveraging boost. Evidently, if v = 1 (no boost), k = 1 (no scaling), lc% = 0 (no leverage), you are left with: `A(t) = A(0)∙(1 + r + α – fc%)^t` since commissions will still have to be paid. Then, you better have some trading skills to outperform the average (α > fc%).
>
> I don't see that as a very pretty picture. The only remedy is to have a positive alpha of sufficient size to compensate for all the drawbacks. Even the one rarely discussed here which is long term return degradation.
>
> I am from the outside, and I would never put anybody's strategy to work without testing the above formula on a sufficiently large portfolio over an extended period of time which is what I do using Q, explore their limits, see how far they can go, where will they break down?
>
> It is like if you can not show me that your trading strategy is scalable upwards, what is its value? If you can show that it can generate some alpha, again what could be its value going forward? And if it is not leverageable, how can we push it further to do even more?
>
> For me, it does not matter which trading methods you use, my interest is only on A(t). And the score for A(t) is kept on the bottom line of the trading account.
>
> However you want to look at it. If you want more, you will have to do more than the other guy.
>
> It's like if we have the exact same car and take a long race to the finish line. Our respective driving skills will matter. But regardless, I could get a definite positive edge just by changing the tires. Then driving skills might not matter much
